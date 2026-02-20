import hmac
import json
import logging
import os
import secrets
import sqlite3
from pathlib import Path

from flask import Flask, abort, g, jsonify, request, session
from werkzeug.utils import secure_filename

from db import apply_migrations
from ingest import ingest_match
from process import UploadProcessor, process_batch

logger = logging.getLogger(__name__)

DB_PATH = Path("db/rl_stats.sqlite")
STATIC_DIR = Path(__file__).parent / "static"
REPLAY_DIR = Path("replays")
MIN_FILE_SIZE = 256 * 1024

ALLOWED_MODES = {"3v3", "2v2", "hoops"}


def _view(prefix, mode):
    if mode not in ALLOWED_MODES:
        raise ValueError(f"Invalid mode: {mode}")
    return f"{prefix}_{mode}"


def query_matches(conn, params):
    page = int(params.get("page", ["1"])[0])
    per_page = min(int(params.get("per_page", ["25"])[0]), 100)
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
    rows = conn.execute(
        """
        SELECT p.name, mp.score, mp.goals, mp.assists, mp.saves, mp.shots,
               CASE WHEN mp.shots > 0
                    THEN ROUND(CAST(mp.goals AS REAL) / mp.shots * 100, 1)
                    ELSE 0 END as shooting_pct
        FROM match_players mp
        JOIN players p ON mp.player_id = p.id
        WHERE mp.match_id = :id
        ORDER BY mp.score DESC
        """,
        {"id": match_id},
    ).fetchall()
    return [dict(r) for r in rows]


def query_shooting_pct(conn, mode):
    view = _view("v_shooting_pct", mode)
    rows = conn.execute(
        f"SELECT player_name AS player, total_goals AS goals, total_shots AS shots, shooting_pct FROM {view} ORDER BY player_name"
    ).fetchall()
    return [dict(r) for r in rows]


def query_win_loss_daily(conn, mode):
    if mode != "3v3":
        return []
    rows = conn.execute(
        "SELECT play_date AS date, wins, losses, win_rate FROM v_win_loss_daily_3v3"
    ).fetchall()
    return [dict(r) for r in rows]


def query_player_stats(conn, mode):
    view = _view("v_player_stats", mode)
    rows = conn.execute(
        f"SELECT player_name AS player, matches_played AS matches, total_goals AS goals, total_assists AS assists, total_saves AS saves, total_shots AS shots FROM {view} ORDER BY player_name"
    ).fetchall()
    return [dict(r) for r in rows]


def query_mvp_wins(conn, mode):
    view = _view("v_mvp_win_rate", mode)
    rows = conn.execute(
        f"SELECT player_name AS player, mvp_matches, mvp_wins, mvp_win_rate AS win_rate FROM {view} ORDER BY player_name"
    ).fetchall()
    return [dict(r) for r in rows]


def query_mvp_losses(conn, mode):
    view = _view("v_mvp_in_losses", mode)
    rows = conn.execute(
        f"SELECT player_name AS player, loss_mvps FROM {view} ORDER BY player_name"
    ).fetchall()
    return [dict(r) for r in rows]


def query_weekday(conn, mode):
    if mode != "3v3":
        return []
    rows = conn.execute(
        "SELECT weekday, matches_played AS matches, wins, losses, win_rate FROM v_win_loss_by_weekday_3v3"
    ).fetchall()
    return [dict(r) for r in rows]


def query_avg_score(conn, mode):
    view = _view("v_avg_score", mode)
    rows = conn.execute(
        f"SELECT player_name AS player, matches_played AS matches, total_score, avg_score FROM {view} ORDER BY player_name"
    ).fetchall()
    return [dict(r) for r in rows]


def query_score_differential(conn, mode):
    view = _view("v_score_differential", mode)
    rows = conn.execute(
        f"SELECT differential, match_count FROM {view} ORDER BY differential"
    ).fetchall()
    return [dict(r) for r in rows]


def query_streaks(conn, mode):
    view = _view("v_streaks", mode)
    row = conn.execute(
        f"SELECT longest_win_streak, longest_loss_streak FROM {view}"
    ).fetchone()
    if row:
        return {
            "longest_win_streak": row["longest_win_streak"] or 0,
            "longest_loss_streak": row["longest_loss_streak"] or 0,
        }
    return {"longest_win_streak": 0, "longest_loss_streak": 0}


def query_avg_goal_contribution(conn, mode):
    view = _view("v_avg_goal_contribution", mode)
    rows = conn.execute(
        f"SELECT player_name AS player, matches_played AS matches, avg_goal_contribution FROM {view} ORDER BY player_name"
    ).fetchall()
    return [dict(r) for r in rows]



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
}


def _get_conn(db_path):
    """Return a per-request read connection, creating it if needed."""
    if "db_conn" not in g:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        g.db_conn = conn
    return g.db_conn


def create_app(db_path, replay_dir=None, processor=None):
    app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="")
    app.secret_key = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
    app.config["MAX_CONTENT_LENGTH"] = 3 * 1024 * 1024
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = True

    upload_dir = replay_dir or REPLAY_DIR

    @app.before_request
    def csrf_check():
        if request.method == "POST":
            token = request.headers.get("X-CSRF-Token", "")
            expected = session.get("csrf_token", "")
            if not expected or not hmac.compare_digest(token, expected):
                return jsonify({"error": "CSRF token missing or invalid"}), 403

    @app.teardown_appcontext
    def close_db(exc):
        conn = g.pop("db_conn", None)
        if conn is not None:
            conn.close()

    @app.route("/")
    def index():
        return app.send_static_file("index.html")

    @app.route("/upload")
    def upload_page():
        return app.send_static_file("upload.html")

    @app.route("/api/auth", methods=["POST"])
    def auth():
        upload_password = os.environ.get("UPLOAD_PASSWORD")
        if not upload_password:
            return jsonify({"error": "Upload disabled"}), 403
        data = request.get_json(silent=True) or {}
        password = data.get("password", "")
        if hmac.compare_digest(password, upload_password):
            session["authenticated"] = True
            return jsonify({"authenticated": True})
        return jsonify({"error": "Wrong password"}), 401

    @app.route("/api/auth/status")
    def auth_status():
        if "csrf_token" not in session:
            session["csrf_token"] = secrets.token_hex(32)
        return jsonify({
            "authenticated": session.get("authenticated", False),
            "csrf_token": session["csrf_token"],
        })

    @app.route("/api/upload", methods=["POST"])
    def upload():
        if not session.get("authenticated"):
            return jsonify({"error": "Not authenticated"}), 401
        if "file" not in request.files:
            return jsonify({"error": "No file provided"}), 400
        f = request.files["file"]
        if not f.filename or not f.filename.lower().endswith(".replay"):
            return jsonify({"error": "Only .replay files are accepted"}), 400
        safe_name = secure_filename(f.filename)
        if not safe_name.lower().endswith(".replay"):
            return jsonify({"error": "Invalid filename"}), 400
        content = f.read()
        if len(content) < MIN_FILE_SIZE:
            return jsonify({"error": f"File too small (minimum {MIN_FILE_SIZE // 1024}KB)"}), 400
        dest = upload_dir / safe_name
        try:
            fd = os.open(str(dest), os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            os.write(fd, content)
            os.close(fd)
        except FileExistsError:
            return jsonify({"error": "File already exists", "duplicate": True}), 409
        if processor is not None:
            processor.enqueue(dest)
        return jsonify({"filename": safe_name}), 201

    @app.route("/api/upload/status")
    def upload_status():
        filename = request.args.get("filename", "")
        if not filename:
            return jsonify({"error": "filename parameter required"}), 400
        safe_name = secure_filename(filename)
        if not safe_name:
            return jsonify({"status": "unknown"})
        replay_path = upload_dir / safe_name
        json_path = replay_path.with_suffix(replay_path.suffix + ".json")
        if json_path.exists():
            return jsonify({"status": "processed"})
        if not replay_path.exists():
            return jsonify({"status": "error"})
        return jsonify({"status": "pending"})

    @app.route("/api/matches")
    def matches():
        conn = _get_conn(db_path)
        params = request.args.to_dict(flat=False)
        try:
            return jsonify(query_matches(conn, params))
        except ValueError, TypeError:
            abort(400)

    @app.route("/api/matches/<int:match_id>/players")
    def match_players(match_id):
        conn = _get_conn(db_path)
        return jsonify(query_match_players(conn, match_id))

    for path, handler_fn in API_ROUTES.items():

        def make_view(fn):
            def view():
                conn = _get_conn(db_path)
                mode = request.args.get("mode", "3v3")
                if mode not in ALLOWED_MODES:
                    mode = "3v3"
                return jsonify(fn(conn, mode))

            return view

        app.add_url_rule(path, endpoint=path, view_func=make_view(handler_fn))

    @app.errorhandler(400)
    def bad_request(e):
        return jsonify({"error": "Bad request"}), 400

    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "Not found"}), 404

    @app.errorhandler(413)
    def too_large(e):
        return jsonify({"error": "File too large (maximum 3MB)"}), 413

    @app.errorhandler(500)
    def internal_error(e):
        return jsonify({"error": "Internal server error"}), 500

    @app.after_request
    def security_headers(response):
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

    return app


def main():
    import os

    from waitress import serve

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    DB_PATH.parent.mkdir(exist_ok=True)

    # Use a temporary connection for startup tasks only
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    apply_migrations(conn)

    # Process any unprocessed replay files at startup
    unprocessed = [
        p for p in REPLAY_DIR.glob("*.replay")
        if not p.with_suffix(p.suffix + ".json").exists()
    ]
    if unprocessed:
        print(f"Processing {len(unprocessed)} unprocessed replay(s) at startup...")
        process_batch(unprocessed, conn)

    # Ingest any parsed JSON files into the database
    json_files = sorted(REPLAY_DIR.glob("*.json"))
    if json_files:
        print(f"Ingesting {len(json_files)} replay JSON file(s)...")
        for path in json_files:
            with open(path, "r", encoding="utf-8") as f:
                replay = json.load(f)
            ingest_match(conn, replay)
        conn.commit()

    conn.close()

    processor = UploadProcessor(DB_PATH)
    app = create_app(DB_PATH, processor=processor)
    print(f"Serving on http://{host}:{port}")
    serve(app, host=host, port=port, threads=8)


if __name__ == "__main__":
    main()
