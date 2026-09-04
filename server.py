import asyncio
import hashlib
import hmac
import logging
import os
import re
import secrets
import sqlite3
from collections.abc import Awaitable, Callable, Generator
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, HTTPException, Query, Request, UploadFile
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

import config
import queries
import replay_view
from db import apply_migrations
from process import process_unprocessed
from upload_processor import UploadProcessor

logger = logging.getLogger(__name__)

_SECURE_RE = re.compile(r"[^\w.-]")
MAX_UPLOAD_BYTES = 5 * 1024 * 1024


def secure_filename(filename: str) -> str:
    name = os.path.basename(filename)
    name = _SECURE_RE.sub("_", name)
    name = name.lstrip(".")
    return name


def validate_upload(filename: str, size: int) -> tuple[str, str | None, int]:
    """Validate a replay upload. Returns (safe_name, error, status_code)."""
    if not filename or not filename.lower().endswith(".replay"):
        return "", "Only .replay files are accepted", 400
    safe_name = secure_filename(filename)
    if not safe_name.lower().endswith(".replay") or safe_name == ".replay":
        return "", "Invalid filename", 400
    if size > MAX_UPLOAD_BYTES:
        return "", "File too large (maximum 5MB)", 413
    return safe_name, None, 200


DB_PATH = Path("db/rl_stats.sqlite")
STATIC_DIR = Path(__file__).parent / "static"
REPLAY_DIR = Path("replays")

_VERSIONED_ASSETS = [
    "app.js",
    "match.js",
    "player.js",
    "replay-core.js",
    "replay.css",
    "replay.js",
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
    tracked_players = settings.players
    tracked_player_names = set(settings.players.values())
    upload_password = settings.upload_password

    upload_dir = replay_dir or REPLAY_DIR
    if processor is None:
        # main() always builds and passes its own processor, so this branch
        # only fires for callers (tests) that don't care about background
        # timing; a long delay keeps it inert instead of firing mid-suite.
        processor = UploadProcessor(db_path, settings.players, upload_dir, delay=3600.0)

    version = _compute_version(STATIC_DIR)
    index_html = _versioned_html(STATIC_DIR / "index.html", version)
    match_html = _versioned_html(STATIC_DIR / "match.html", version)
    player_html = _versioned_html(STATIC_DIR / "player.html", version)
    upload_html = _versioned_html(STATIC_DIR / "upload.html", version)
    replay_html = _versioned_html(STATIC_DIR / "replay.html", version)

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
    # Outermost: compresses the large replay-viewer payloads (position buffer,
    # frame-time array) and every other response over the threshold.
    app.add_middleware(GZipMiddleware, minimum_size=1024)

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

    @app.get("/match/{match_id}/replay")
    async def replay_page(
        match_id: int, conn: Annotated[sqlite3.Connection, Depends(get_conn)]
    ):
        if replay_view.replay_path_for(conn, upload_dir, match_id) is None:
            raise HTTPException(status_code=404, detail="No replay for this match")
        return HTMLResponse(replay_html)

    # replay.js is the one ES module that imports same-dir siblings; the
    # StaticFiles mount would serve those bare specifiers uncached, so a
    # replay-core.js change could be masked by a stale browser cache. Serve
    # replay.js here with every `./x.js` import version-stamped, same as
    # _versioned_html does for HTML. (An inline <script type=importmap> would be
    # the other way to pin them, but CSP `script-src 'self' cdn.jsdelivr.net`
    # blocks inline script without a hash/nonce — don't "simplify" to that.)
    replay_js = re.sub(
        r'(from "\./[\w-]+\.js)"',
        rf'\1?v={version}"',
        (STATIC_DIR / "replay.js").read_text(),
    )
    replay_js_etag = f'"{version}"'

    @app.get("/static/replay.js")
    async def replay_js_route(request: Request):
        # StaticFiles would send ETag + answer conditional GETs; do the same by
        # hand for this rewritten copy so a revalidation is a 304, not a full 200.
        if request.headers.get("if-none-match") == replay_js_etag:
            return Response(status_code=304, headers={"ETag": replay_js_etag})
        return Response(
            replay_js,
            media_type="text/javascript",
            headers={"Cache-Control": "no-cache", "ETag": replay_js_etag},
        )

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
        safe_name, error, status_code = validate_upload(
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

        status = processor.status(safe_name)
        result: dict[str, Any] = {"status": status.state.value}
        if status.error is not None:
            result["error"] = status.error
        if status.reason is not None:
            result["reason"] = status.reason
        if status.stage is not None:
            result["stage"] = status.stage
        if status.batch is not None:
            result["batch"] = {
                "completed": status.batch[0],
                "total": status.batch[1],
            }
        return result

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
    ) -> Any:
        return queries.matches(
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
    ) -> Any:
        return queries.match_players(conn, match_id)

    @app.get("/api/matches/{match_id}")
    async def match_detail(
        match_id: int, conn: Annotated[sqlite3.Connection, Depends(get_conn)]
    ) -> Any:
        data = queries.match_detail(conn, match_id)
        if data is None:
            raise HTTPException(status_code=404, detail="Not found")
        return data

    # -- Replay viewer routes --

    @app.get("/api/matches/{match_id}/has-replay")
    async def match_has_replay(
        match_id: int, conn: Annotated[sqlite3.Connection, Depends(get_conn)]
    ) -> Any:
        """Cheap probe for link-gating on the match page: no rrrocket, no parse."""
        path = replay_view.replay_path_for(conn, upload_dir, match_id)
        return {"has_replay": path is not None}

    @app.get("/api/matches/{match_id}/replay")
    async def match_replay(
        match_id: int, conn: Annotated[sqlite3.Connection, Depends(get_conn)]
    ) -> Response:
        """The merged replay envelope: meta + positions + boost from a single
        rrrocket parse. See docs/adr/0004's addendum — this used to be 3 routes,
        each re-parsing the file from scratch."""
        path = replay_view.replay_path_for(conn, upload_dir, match_id)
        if path is None:
            raise HTTPException(status_code=404, detail="No replay for this match")
        # Only the parse goes to a worker thread — replay_path_for's DB read
        # above is sub-millisecond, nothing to gain by moving it too. Without
        # this, rrrocket's ~250ms subprocess call blocks the whole event loop.
        frames = await asyncio.to_thread(
            replay_view.build_replay_frames, path, tracked_players
        )
        if frames is None:
            raise HTTPException(status_code=422, detail="Replay could not be read")
        return Response(
            content=replay_view.serialize_replay_envelope(frames),
            media_type="application/octet-stream",
            headers={"Cache-Control": "no-store"},
        )

    # -- Stats routes --

    def game_mode(mode: str = "3v3") -> str:
        return mode if mode in ALLOWED_MODES else "3v3"

    def make_stat_handler(slug: str) -> Any:
        async def view(
            mode: Annotated[str, Depends(game_mode)],
            conn: Annotated[sqlite3.Connection, Depends(get_conn)],
        ) -> Any:
            return queries.stats(slug, conn, mode)

        return view

    for slug in queries.STAT_READS:
        path = f"/api/stats/{slug}"
        app.get(path, name=path)(make_stat_handler(slug))

    # These return typed results from queries.*; the route annotations stay
    # loose (-> Any) so FastAPI serializes via jsonable_encoder without
    # building a response_model, matching the rest of this app.
    @app.get("/api/stats/timeline")
    async def timeline(
        mode: Annotated[str, Depends(game_mode)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
    ) -> Any:
        return queries.timeline(conn, mode)

    @app.get("/api/stats/streaks")
    async def streaks(
        mode: Annotated[str, Depends(game_mode)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
    ) -> Any:
        return queries.streaks(conn, mode)

    @app.get("/api/stats/goal-timing")
    async def goal_timing(
        mode: Annotated[str, Depends(game_mode)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
    ) -> Any:
        return queries.goal_timing(conn, mode)

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
    ) -> Any:
        return queries.player_career(conn, player_name, mode)

    @app.get("/api/players/{player_name}/time-series")
    async def player_time_series_route(
        player_name: Annotated[str, Depends(get_tracked_player)],
        conn: Annotated[sqlite3.Connection, Depends(get_conn)],
        mode: Annotated[str, Depends(game_mode)],
    ) -> Any:
        return queries.player_time_series(conn, player_name, mode)

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

    processor = UploadProcessor(DB_PATH, settings.players, REPLAY_DIR)
    app = create_app(DB_PATH, processor=processor, settings=settings)
    print(f"Serving on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
