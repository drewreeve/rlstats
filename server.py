import sqlite3
from pathlib import Path

from flask import Flask, abort, jsonify, request

from db import apply_migrations

DB_PATH = Path("db/rl_stats.sqlite")
STATIC_DIR = Path(__file__).parent / "static"

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
        where.append("p.name LIKE :search")
        bindings["search"] = f"%{search}%"

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
}


def create_app(conn):
    app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="")

    @app.route("/")
    def index():
        return app.send_static_file("index.html")

    @app.route("/api/matches")
    def matches():
        params = request.args.to_dict(flat=False)
        try:
            return jsonify(query_matches(conn, params))
        except (ValueError, TypeError):
            abort(400)

    @app.route("/api/matches/<int:match_id>/players")
    def match_players(match_id):
        return jsonify(query_match_players(conn, match_id))

    for path, handler_fn in API_ROUTES.items():

        def make_view(fn):
            def view():
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
            "font-src 'self' https://fonts.gstatic.com; "
            "connect-src 'self'; "
            "img-src 'self'"
        )
        return response

    return app


def main():
    import os

    from waitress import serve

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run ingest.py first.")
        raise SystemExit(1)

    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    apply_migrations(conn)

    app = create_app(conn)
    print(f"Serving on http://{host}:{port}")
    serve(app, host=host, port=port)


if __name__ == "__main__":
    main()
