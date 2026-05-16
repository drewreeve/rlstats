import hashlib
import hmac
import logging
import os
import re
import secrets
import sqlite3
from collections.abc import Awaitable, Callable, Generator
from itertools import groupby
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

import config
from db import apply_migrations, queries
from process import UploadProcessor, process_unprocessed
from replay_validator import secure_filename
from replay_validator import validate as validate_replay

logger = logging.getLogger(__name__)

DB_PATH = Path("db/rl_stats.sqlite")
STATIC_DIR = Path(__file__).parent / "static"
REPLAY_DIR = Path("replays")

_VERSIONED_ASSETS = [
    "app.js",
    "match.js",
    "player.js",
    "style.css",
    "upload.js",
    "utils.js",
]


def _compute_version(static_dir: Path) -> str:
    h = hashlib.sha256()
    for name in _VERSIONED_ASSETS:
        h.update((static_dir / name).read_bytes())
    return h.hexdigest()[:12]


def _versioned_html(path: Path, version: str) -> str:
    content = path.read_text()
    return re.sub(r'(/static/[^"]+\.(?:css|js))', rf"\1?v={version}", content)


ALLOWED_MODES = {"3v3", "2v2", "hoops"}


def query_matches(
    conn: sqlite3.Connection,
    *,
    page: int,
    per_page: int,
    search: str,
    game_mode: str,
    result: str,
    date_from: str,
    date_to: str,
) -> dict[str, Any]:
    offset = (page - 1) * per_page

    where: list[str] = []
    bindings: dict[str, Any] = {}
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
    if date_from:
        where.append("m.played_at >= :date_from")
        bindings["date_from"] = date_from
    if date_to:
        where.append("m.played_at < :date_to")
        bindings["date_to"] = date_to

    where_clause = (" AND " + " AND ".join(where)) if where else ""

    count_sql = f"""
        SELECT COUNT(*) FROM matches m
        LEFT JOIN players p ON m.team_mvp_player_id = p.id
        WHERE 1=1{where_clause}
    """
    total = conn.execute(count_sql, bindings).fetchone()[0]

    query_sql = f"""
        SELECT m.id, m.game_mode, m.result, m.forfeit,
               m.team_score || '-' || m.opponent_score AS score,
               m.played_at, p.name AS mvp
        FROM matches m
        LEFT JOIN players p ON m.team_mvp_player_id = p.id
        WHERE 1=1{where_clause}
        ORDER BY m.played_at DESC
        LIMIT :per_page OFFSET :offset
    """
    bindings["per_page"] = per_page
    bindings["offset"] = offset

    rows = conn.execute(query_sql, bindings).fetchall()
    matches = [dict(r) for r in rows]
    return {"matches": matches, "total": total, "page": page, "per_page": per_page}


def query_match_players(
    conn: sqlite3.Connection, match_id: int
) -> list[dict[str, Any]]:
    rows = queries.match_players(conn, match_id=match_id)
    return [dict(r) for r in rows]


def query_match_detail(
    conn: sqlite3.Connection, match_id: int
) -> dict[str, Any] | None:
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


STAT_ROUTES = {
    "/api/stats/shooting": queries.shooting_pct,
    "/api/stats/players": queries.player_stats,
    "/api/stats/mvp-wins": queries.mvp_wins,
    "/api/stats/mvp-losses": queries.mvp_losses,
    "/api/stats/weekday": queries.weekday,
    "/api/stats/avg-score": queries.avg_score,
    "/api/stats/score-differential": queries.score_differential,
    "/api/stats/goal-contributions": queries.avg_goal_contribution,
    "/api/stats/score-range": queries.score_range,
    "/api/stats/offensive-pairings": queries.offensive_pairings,
}


def compute_goal_timing(
    events: list[Any],
) -> tuple[float | None, float | None]:
    """Returns (avg_seconds_to_concede_after_scoring, avg_lead_duration)."""
    concede_delays: list[float] = []
    lead_durations: list[float] = []
    for _, match_events in groupby(events, key=lambda e: e["match_id"]):
        our, opp, lead_start, duration = 0, 0, None, None
        prev_is_ours: bool | None = None
        prev_time: float | None = None
        for ev in match_events:
            duration = ev["duration_seconds"]
            is_ours = bool(ev["is_ours"])
            was_leading = our > opp
            if not is_ours and prev_is_ours and prev_time is not None:
                concede_delays.append(ev["game_seconds"] - prev_time)
            if is_ours:
                our += 1
            else:
                opp += 1
            is_leading = our > opp
            if not was_leading and is_leading:
                lead_start = ev["game_seconds"]
            elif was_leading and not is_leading:
                if lead_start is not None:
                    lead_durations.append(ev["game_seconds"] - lead_start)
                lead_start = None
            prev_is_ours = is_ours
            prev_time = ev["game_seconds"]
        if our > opp and lead_start is not None and duration:
            lead_durations.append(duration - lead_start)
    avg_concede = sum(concede_delays) / len(concede_delays) if concede_delays else None
    avg_lead = sum(lead_durations) / len(lead_durations) if lead_durations else None
    return avg_concede, avg_lead


def _get_conn(db_path: str | Path) -> sqlite3.Connection:
    """Open a read connection to the database."""
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def create_app(
    db_path: str | Path,
    replay_dir: Path | None = None,
    processor: UploadProcessor | None = None,
    settings: config.Settings | None = None,
) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None)

    if settings is None:
        settings = config.load_settings()
    tracked_player_names = set(settings.players.values())
    upload_password = settings.upload_password

    upload_dir = replay_dir or REPLAY_DIR

    version = _compute_version(STATIC_DIR)
    index_html = _versioned_html(STATIC_DIR / "index.html", version)
    match_html = _versioned_html(STATIC_DIR / "match.html", version)
    player_html = _versioned_html(STATIC_DIR / "player.html", version)
    upload_html = _versioned_html(STATIC_DIR / "upload.html", version)

    def get_conn() -> Generator[sqlite3.Connection, None, None]:
        conn = _get_conn(db_path)
        try:
            yield conn
        finally:
            conn.close()

    @app.middleware("http")  # pyright: ignore[reportUnusedFunction]
    async def security_headers(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
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

    @app.middleware("http")  # pyright: ignore[reportUnusedFunction]
    async def csrf_check(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
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
    secret_key = settings.secret_key or secrets.token_hex(32)
    app.add_middleware(
        SessionMiddleware,  # type: ignore[arg-type]
        secret_key=secret_key,
        https_only=True,
    )

    # -- HTML page routes --

    for html_path in ["/", "/2v2", "/hoops", "/history"]:

        def _make_index(p: str = html_path):
            async def _index():
                return HTMLResponse(index_html)

            _index.__name__ = f"index_{p.strip('/')}" if p != "/" else "index_root"
            return _index

        app.get(html_path)(_make_index())

    @app.get("/upload")
    async def upload_page():
        return HTMLResponse(upload_html)

    @app.get("/match/{match_id}")
    async def match_page(match_id: int):
        return HTMLResponse(match_html)

    # -- Auth routes --

    @app.post("/api/auth")
    async def auth(request: Request):
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
    async def matches(
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
        page: int = Query(1, ge=1),
        per_page: int = Query(25, ge=1, le=100),
        search: str = "",
        game_mode: str = "",
        result: str = "",
        date_from: str = "",
        date_to: str = "",
    ):
        return query_matches(
            conn,
            page=page,
            per_page=per_page,
            search=search,
            game_mode=game_mode,
            result=result,
            date_from=date_from,
            date_to=date_to,
        )

    @app.get("/api/matches/{match_id}/players")
    async def match_players_route(
        match_id: int, conn: Annotated[sqlite3.Connection, Depends(get_conn)]
    ):
        return query_match_players(conn, match_id)

    @app.get("/api/matches/{match_id}")
    async def match_detail(
        match_id: int, conn: Annotated[sqlite3.Connection, Depends(get_conn)]
    ):
        data = query_match_detail(conn, match_id)
        if data is None:
            raise HTTPException(status_code=404, detail="Not found")
        return data

    # -- Stats routes --

    def game_mode(mode: str = "3v3") -> str:
        return mode if mode in ALLOWED_MODES else "3v3"

    def make_stat_handler(fn: Any) -> Any:
        async def view(
            mode: Annotated[str, Depends(game_mode)],
            conn: Annotated[sqlite3.Connection, Depends(get_conn)],
        ) -> list[dict[str, Any]]:
            rows: Any = fn(conn, game_mode=mode)
            return [dict(r) for r in rows]

        return view

    for path, handler_fn in STAT_ROUTES.items():
        app.get(path, name=path)(make_stat_handler(handler_fn))

    @app.get("/api/stats/timeline")
    async def timeline(
        mode: Annotated[str, Depends(game_mode)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
    ) -> list[dict[str, Any]]:
        if mode in ("2v2", "hoops"):
            rows = queries.win_loss_daily_pairings(conn, game_mode=mode)
        else:
            rows = queries.win_loss_daily(conn, game_mode=mode)
        return [dict(r) for r in rows]

    @app.get("/api/stats/streaks")
    async def streaks(
        mode: Annotated[str, Depends(game_mode)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
    ):
        row = next(iter(queries.streaks(conn, game_mode=mode)), None)
        if row:
            return {
                "longest_win_streak": row["longest_win_streak"] or 0,
                "longest_loss_streak": row["longest_loss_streak"] or 0,
            }
        return {"longest_win_streak": 0, "longest_loss_streak": 0}

    @app.get("/api/stats/goal-timing")
    async def goal_timing(
        mode: Annotated[str, Depends(game_mode)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
    ):
        events = list(queries.goal_events_for_mode(conn, game_mode=mode))
        avg_concede, avg_lead = compute_goal_timing(events)
        return {
            "avg_seconds_to_concede": round(avg_concede)
            if avg_concede is not None
            else None,
            "avg_lead_duration": round(avg_lead) if avg_lead is not None else None,
        }

    # -- Player routes --

    def get_tracked_player(player_name: str) -> str:
        if player_name not in tracked_player_names:
            raise HTTPException(status_code=404, detail="Player not found")
        return player_name

    @app.get("/player/{player_name}")
    async def player_page(player_name: Annotated[str, Depends(get_tracked_player)]):
        return HTMLResponse(player_html)

    @app.get("/api/players/{player_name}")
    async def player_career(
        player_name: Annotated[str, Depends(get_tracked_player)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
        mode: Annotated[str, Depends(game_mode)],
    ):
        row = queries.player_career_stats(conn, player_name=player_name, game_mode=mode)
        if row is None:
            return {
                "player": player_name,
                "matches": 0,
                "goals": 0,
                "assists": 0,
                "saves": 0,
                "shots": 0,
                "demos": 0,
                "avg_score": None,
                "shooting_pct": None,
                "mvp_count": 0,
                "wins": 0,
                "losses": 0,
                "avg_boost_per_minute": None,
                "avg_supersonic_pct": None,
                "avg_demos": None,
                "avg_demos_received": None,
            }
        return dict(row)

    @app.get("/api/players/{player_name}/time-series")
    async def player_time_series_route(
        player_name: Annotated[str, Depends(get_tracked_player)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
        mode: Annotated[str, Depends(game_mode)],
    ):
        rows = queries.player_time_series(conn, player_name=player_name, game_mode=mode)
        return [dict(r) for r in rows]

    # -- Exception handlers --

    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse({"error": exc.detail}, status_code=exc.status_code)

    # -- Static files (must be last) --

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app


def main():
    import os

    import uvicorn

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    DB_PATH.parent.mkdir(exist_ok=True)

    conn = _get_conn(DB_PATH)
    apply_migrations(conn)
    conn.close()

    settings = config.load_settings()
    process_unprocessed(DB_PATH, REPLAY_DIR, settings.players)

    processor = UploadProcessor(DB_PATH, settings.players)
    app = create_app(DB_PATH, processor=processor, settings=settings)
    print(f"Serving on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
