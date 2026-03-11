import hmac
import logging
import os
import secrets
import sqlite3
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from db import apply_migrations, queries
from ingest import analyze_replay, write_match
from process import UploadProcessor, parse_replay
from replay_validator import secure_filename, validate as validate_replay

logger = logging.getLogger(__name__)

DB_PATH = Path("db/rl_stats.sqlite")
STATIC_DIR = Path(__file__).parent / "static"
REPLAY_DIR = Path("replays")
ALLOWED_MODES = {"3v3", "2v2", "hoops"}


def query_matches(conn, params):
    page = max(1, int(params.get("page", ["1"])[0]))
    per_page = max(1, min(int(params.get("per_page", ["25"])[0]), 100))
    search = params.get("search", [""])[0].strip()
    game_mode = params.get("game_mode", [""])[0].strip()
    result = params.get("result", [""])[0].strip()
    offset = (page - 1) * per_page

    where = []
    bindings = {}
    if game_mode:
        where.append("m.game_mode = :game_mode")
        bindings["game_mode"] = game_mode
    if result:
        where.append("m.result = :result")
        bindings["result"] = result
    if search:
        where.append("p.name LIKE :search ESCAPE '\\'")
        escaped = search.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        bindings["search"] = f"%{escaped}%"

    date_from = params.get("date_from", [""])[0].strip()
    date_to = params.get("date_to", [""])[0].strip()
    if date_from:
        where.append("m.played_at >= :date_from")
        bindings["date_from"] = date_from
    if date_to:
        where.append("m.played_at < date(:date_to, '+1 day')")
        bindings["date_to"] = date_to

    where_clause = (" AND " + " AND ".join(where)) if where else ""

    count_sql = f"""
        SELECT COUNT(*) FROM matches m
        LEFT JOIN players p ON m.team_mvp_player_id = p.id
        WHERE 1=1{where_clause}
    """
    total = conn.execute(count_sql, bindings).fetchone()[0]

    query_sql = f"""
        SELECT m.id, m.game_mode, m.result, m.forfeit, m.team_score,
               m.opponent_score, m.played_at, p.name as mvp_name
        FROM matches m
        LEFT JOIN players p ON m.team_mvp_player_id = p.id
        WHERE 1=1{where_clause}
        ORDER BY m.played_at DESC
        LIMIT :per_page OFFSET :offset
    """
    bindings["per_page"] = per_page
    bindings["offset"] = offset

    rows = conn.execute(query_sql, bindings).fetchall()
    matches = [
        {
            "id": r["id"],
            "game_mode": r["game_mode"],
            "result": r["result"],
            "forfeit": r["forfeit"],
            "score": f"{r['team_score']}-{r['opponent_score']}",
            "played_at": r["played_at"],
            "mvp": r["mvp_name"],
        }
        for r in rows
    ]
    return {"matches": matches, "total": total, "page": page, "per_page": per_page}


def query_match_players(conn, match_id):
    rows = queries.match_players(conn, match_id=match_id)
    return [dict(r) for r in rows]


def query_match_detail(conn, match_id):
    match = queries.match_metadata(conn, match_id=match_id)
    if not match:
        return None

    players = list(queries.match_players(conn, match_id=match_id))

    team_num = match["team"]
    team_players = [dict(p) for p in players if p["team"] == team_num]
    opponent_players = [dict(p) for p in players if p["team"] != team_num]

    events = [
        {
            "event_type": e["event_type"],
            "game_seconds": e["game_seconds"],
            "team": e["team"],
            "name": e["name"],
        }
        for e in queries.match_events(conn, match_id=match_id)
    ]

    return {
        "match": {
            "id": match["id"],
            "played_at": match["played_at"],
            "game_mode": match["game_mode"],
            "result": match["result"],
            "forfeit": match["forfeit"],
            "team_score": match["team_score"],
            "opponent_score": match["opponent_score"],
            "duration_seconds": match["duration_seconds"],
            "team": team_num,
            "team_possession_seconds": match["team_possession_seconds"],
            "opponent_possession_seconds": match["opponent_possession_seconds"],
            "defensive_third_seconds": match["defensive_third_seconds"],
            "neutral_third_seconds": match["neutral_third_seconds"],
            "offensive_third_seconds": match["offensive_third_seconds"],
            "team_boost_collected": match["team_boost_collected"],
            "opponent_boost_collected": match["opponent_boost_collected"],
            "team_boost_stolen": match["team_boost_stolen"],
            "opponent_boost_stolen": match["opponent_boost_stolen"],
        },
        "events": events,
        "team_players": team_players,
        "opponent_players": opponent_players,
    }


def query_shooting_pct(conn, mode):
    rows = queries.shooting_pct(conn, game_mode=mode)
    return [
        {
            "player": r["player_name"],
            "goals": r["total_goals"],
            "shots": r["total_shots"],
            "shooting_pct": r["shooting_pct"],
        }
        for r in rows
    ]


def query_win_loss_daily(conn, mode):
    rows = queries.win_loss_daily(conn, game_mode=mode)
    return [
        {
            "date": r["play_date"],
            "wins": r["wins"],
            "losses": r["losses"],
            "win_rate": r["win_rate"],
        }
        for r in rows
    ]


def query_player_stats(conn, mode):
    rows = queries.player_stats(conn, game_mode=mode)
    return [
        {
            "player": r["player_name"],
            "matches": r["matches_played"],
            "goals": r["total_goals"],
            "assists": r["total_assists"],
            "saves": r["total_saves"],
            "shots": r["total_shots"],
            "demos": r["total_demos"],
        }
        for r in rows
    ]


def query_mvp_wins(conn, mode):
    rows = queries.mvp_wins(conn, game_mode=mode)
    return [
        {
            "player": r["player_name"],
            "mvp_matches": r["mvp_matches"],
            "mvp_wins": r["mvp_wins"],
            "win_rate": r["mvp_win_rate"],
        }
        for r in rows
    ]


def query_mvp_losses(conn, mode):
    rows = queries.mvp_losses(conn, game_mode=mode)
    return [{"player": r["player_name"], "loss_mvps": r["loss_mvps"]} for r in rows]


def query_weekday(conn, mode):
    rows = queries.weekday(conn, game_mode=mode)
    return [
        {
            "weekday": r["weekday"],
            "matches": r["matches_played"],
            "wins": r["wins"],
            "losses": r["losses"],
            "win_rate": r["win_rate"],
        }
        for r in rows
    ]


def query_avg_score(conn, mode):
    rows = queries.avg_score(conn, game_mode=mode)
    return [
        {
            "player": r["player_name"],
            "matches": r["matches_played"],
            "total_score": r["total_score"],
            "avg_score": r["avg_score"],
        }
        for r in rows
    ]


def query_score_differential(conn, mode):
    rows = queries.score_differential(conn, game_mode=mode)
    return [dict(r) for r in rows]


def query_streaks(conn, mode):
    rows = list(queries.streaks(conn, game_mode=mode))
    if rows:
        row = rows[0]
        return {
            "longest_win_streak": row["longest_win_streak"] or 0,
            "longest_loss_streak": row["longest_loss_streak"] or 0,
        }
    return {"longest_win_streak": 0, "longest_loss_streak": 0}


def query_avg_goal_contribution(conn, mode):
    rows = queries.avg_goal_contribution(conn, game_mode=mode)
    return [
        {
            "player": r["player_name"],
            "matches": r["matches_played"],
            "avg_goal_contribution": r["avg_goal_contribution"],
        }
        for r in rows
    ]


def query_score_range(conn, mode):
    rows = queries.score_range(conn, game_mode=mode)
    return [
        {"player": r["player_name"], "min": r["min_score"], "max": r["max_score"]}
        for r in rows
    ]


API_ROUTES = {
    "/api/shooting-pct": query_shooting_pct,
    "/api/win-loss-daily": query_win_loss_daily,
    "/api/player-stats": query_player_stats,
    "/api/mvp-wins": query_mvp_wins,
    "/api/mvp-losses": query_mvp_losses,
    "/api/weekday": query_weekday,
    "/api/avg-score": query_avg_score,
    "/api/score-differential": query_score_differential,
    "/api/streaks": query_streaks,
    "/api/avg-goal-contribution": query_avg_goal_contribution,
    "/api/score-range": query_score_range,
}


def _get_conn(db_path):
    """Open a read connection to the database."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def create_app(db_path, replay_dir=None, processor=None):
    app = FastAPI(docs_url=None, redoc_url=None)

    upload_dir = replay_dir or REPLAY_DIR

    def get_conn():
        conn = _get_conn(db_path)
        try:
            yield conn
        finally:
            conn.close()

    @app.middleware("http")
    async def security_headers(request: Request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' https://cdn.jsdelivr.net; "
            "style-src 'self' https://fonts.googleapis.com; "
            "style-src-attr 'unsafe-inline'; "
            "font-src 'self' https://fonts.gstatic.com; "
            "connect-src 'self'; "
            "img-src 'self' data:; "
            "base-uri 'self'; "
            "form-action 'self'; "
            "frame-ancestors 'none'; "
            "object-src 'none'"
        )
        return response

    @app.middleware("http")
    async def csrf_check(request: Request, call_next):
        if request.method == "POST":
            token = request.headers.get("X-CSRF-Token", "")
            expected = request.session.get("csrf_token", "")
            if not expected or not hmac.compare_digest(token, expected):
                return JSONResponse(
                    {"error": "CSRF token missing or invalid"}, status_code=403
                )
        return await call_next(request)

    # SessionMiddleware must be added AFTER @app.middleware("http") decorators
    # because add_middleware inserts at position 0, making the last-added
    # middleware outermost. Session must wrap CSRF so request.session is available.
    secret_key = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
    app.add_middleware(
        SessionMiddleware,  # type: ignore[arg-type]
        secret_key=secret_key,
        https_only=True,
    )

    # -- HTML page routes --

    index_path = str(STATIC_DIR / "index.html")
    for html_path in ["/", "/2v2", "/hoops", "/history"]:

        def _make_index(p=html_path):
            async def _index():
                return FileResponse(index_path)

            _index.__name__ = f"index_{p.strip('/')}" if p != "/" else "index_root"
            return _index

        app.get(html_path)(_make_index())

    @app.get("/upload")
    async def upload_page():
        return FileResponse(str(STATIC_DIR / "upload.html"))

    @app.get("/match/{match_id}")
    async def match_page(match_id: int):
        return FileResponse(str(STATIC_DIR / "match.html"))

    # -- Auth routes --

    @app.post("/api/auth")
    async def auth(request: Request):
        upload_password = os.environ.get("UPLOAD_PASSWORD")
        if not upload_password:
            return JSONResponse({"error": "Upload disabled"}, status_code=403)
        data = await request.json()
        password = data.get("password", "")
        if hmac.compare_digest(password, upload_password):
            request.session["authenticated"] = True
            return {"authenticated": True}
        return JSONResponse({"error": "Wrong password"}, status_code=401)

    @app.get("/api/auth/status")
    async def auth_status(request: Request):
        if "csrf_token" not in request.session:
            request.session["csrf_token"] = secrets.token_hex(32)
        return {
            "authenticated": request.session.get("authenticated", False),
            "csrf_token": request.session["csrf_token"],
        }

    # -- Upload routes --

    @app.post("/api/upload")
    async def upload(request: Request, file: UploadFile | None = None):
        if not request.session.get("authenticated"):
            return JSONResponse({"error": "Not authenticated"}, status_code=401)
        if file is None:
            return JSONResponse({"error": "No file provided"}, status_code=400)
        content = await file.read()
        safe_name, error, status_code = validate_replay(
            file.filename or "", len(content)
        )
        if error:
            return JSONResponse({"error": error}, status_code=status_code)
        dest = upload_dir / safe_name
        try:
            fd = os.open(str(dest), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            try:
                os.write(fd, content)
            finally:
                os.close(fd)
        except FileExistsError:
            return JSONResponse(
                {"error": "File already exists", "duplicate": True}, status_code=409
            )
        if processor is not None:
            processor.enqueue(dest)
        return JSONResponse({"filename": safe_name}, status_code=201)

    @app.get("/api/upload/status")
    async def upload_status(request: Request):
        filename = request.query_params.get("filename", "")
        if not filename:
            return JSONResponse(
                {"error": "filename parameter required"}, status_code=400
            )
        safe_name = secure_filename(filename)
        if not safe_name:
            return {"status": "unknown"}
        replay_path = upload_dir / safe_name
        ingested_path = replay_path.with_suffix(replay_path.suffix + ".ingested")
        if ingested_path.exists():
            return {"status": "processed"}
        if not replay_path.exists():
            return {"status": "error"}
        return {"status": "pending"}

    # -- Match routes --

    @app.get("/api/matches")
    async def matches(request: Request, conn=Depends(get_conn)):
        params: dict[str, list[str]] = {}
        for k, v in request.query_params.multi_items():
            params.setdefault(k, []).append(v)
        try:
            return query_matches(conn, params)
        except ValueError, TypeError:
            raise HTTPException(status_code=400, detail="Bad request")

    @app.get("/api/matches/{match_id}/players")
    async def match_players_route(match_id: int, conn=Depends(get_conn)):
        return query_match_players(conn, match_id)

    @app.get("/api/matches/{match_id}")
    async def match_detail(match_id: int, conn=Depends(get_conn)):
        data = query_match_detail(conn, match_id)
        if data is None:
            raise HTTPException(status_code=404, detail="Not found")
        return data

    # -- Stats routes --

    for path, handler_fn in API_ROUTES.items():

        def make_view(fn):
            async def view(mode: str = "3v3", conn=Depends(get_conn)):
                if mode not in ALLOWED_MODES:
                    mode = "3v3"
                return fn(conn, mode)

            return view

        app.get(path, name=path)(make_view(handler_fn))

    # -- Exception handlers --

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    # -- Static files (must be last) --

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app


def _parse_and_analyze(replay_path):
    """Worker for parallel startup: parse + analyze a replay without DB access."""
    replay, error = parse_replay(replay_path)
    if replay is None:
        return None
    return analyze_replay(replay)


def main():
    import os

    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    DB_PATH.parent.mkdir(exist_ok=True)

    # Use a temporary connection for startup tasks only
    conn = _get_conn(DB_PATH)
    apply_migrations(conn)

    # Parse and ingest any unprocessed replay files
    unprocessed = [
        p
        for p in REPLAY_DIR.glob("*.replay")
        if not p.with_suffix(p.suffix + ".ingested").exists()
    ]
    if unprocessed:
        replay_paths = sorted(unprocessed)
        print(f"Processing {len(replay_paths)} unprocessed replay(s) at startup...")
        with ProcessPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(_parse_and_analyze, replay_paths))
        ingested = []
        for path, analysis in zip(replay_paths, results):
            if analysis is not None:
                write_match(conn, analysis)
                ingested.append(path)
        conn.commit()
        for replay_path in ingested:
            replay_path.with_suffix(replay_path.suffix + ".ingested").touch()

    conn.close()

    processor = UploadProcessor(DB_PATH)
    app = create_app(DB_PATH, processor=processor)
    print(f"Serving on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
