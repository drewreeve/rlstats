import json
import sqlite3
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from db import apply_migrations

DB_PATH = Path("db/rl_stats.sqlite")
STATIC_DIR = Path(__file__).parent / "static"

ALLOWED_MODES = {"3v3", "2v2", "hoops"}


def query_matches(conn, params):
    page = int(params.get("page", ["1"])[0])
    per_page = int(params.get("per_page", ["20"])[0])
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
            "id": r[0],
            "game_mode": r[1],
            "result": r[2],
            "forfeit": r[3],
            "score": f"{r[4]}-{r[5]}",
            "played_at": r[6],
            "mvp": r[7],
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
        WHERE mp.match_id = :id AND p.name IN ('Drew', 'Steve', 'Jeff')
        ORDER BY mp.score DESC
        """,
        {"id": match_id},
    ).fetchall()
    return [
        {
            "name": r[0],
            "score": r[1],
            "goals": r[2],
            "assists": r[3],
            "saves": r[4],
            "shots": r[5],
            "shooting_pct": r[6],
        }
        for r in rows
    ]


def query_shooting_pct(conn, mode):
    view = f"v_shooting_pct_{mode}"
    rows = conn.execute(
        f"SELECT player_name, total_goals, total_shots, shooting_pct FROM {view} ORDER BY player_name"
    ).fetchall()
    return [
        {"player": r[0], "goals": r[1], "shots": r[2], "shooting_pct": r[3]}
        for r in rows
    ]


def query_win_loss_daily(conn, mode):
    if mode != "3v3":
        return []
    rows = conn.execute(
        "SELECT play_date, wins, losses, win_rate FROM v_win_loss_daily_3v3"
    ).fetchall()
    return [
        {"date": r[0], "wins": r[1], "losses": r[2], "win_rate": r[3]} for r in rows
    ]


def query_player_stats(conn, mode):
    view = f"v_player_stats_{mode}"
    rows = conn.execute(
        f"SELECT player_name, matches_played, total_goals, total_assists, total_saves, total_shots FROM {view} ORDER BY player_name"
    ).fetchall()
    return [
        {
            "player": r[0],
            "matches": r[1],
            "goals": r[2],
            "assists": r[3],
            "saves": r[4],
            "shots": r[5],
        }
        for r in rows
    ]


def query_mvp_wins(conn, mode):
    view = f"v_mvp_win_rate_{mode}"
    rows = conn.execute(
        f"SELECT player_name, mvp_matches, mvp_wins, mvp_win_rate FROM {view} ORDER BY player_name"
    ).fetchall()
    return [
        {"player": r[0], "mvp_matches": r[1], "mvp_wins": r[2], "win_rate": r[3]}
        for r in rows
    ]


def query_mvp_losses(conn, mode):
    view = f"v_mvp_in_losses_{mode}"
    rows = conn.execute(
        f"SELECT player_name, loss_mvps FROM {view} ORDER BY player_name"
    ).fetchall()
    return [{"player": r[0], "loss_mvps": r[1]} for r in rows]


def query_weekday(conn, mode):
    if mode != "3v3":
        return []
    rows = conn.execute(
        "SELECT weekday, matches_played, wins, losses, win_rate FROM v_win_loss_by_weekday_3v3"
    ).fetchall()
    return [
        {
            "weekday": r[0],
            "matches": r[1],
            "wins": r[2],
            "losses": r[3],
            "win_rate": r[4],
        }
        for r in rows
    ]


def query_avg_score(conn, mode):
    view = f"v_avg_score_{mode}"
    rows = conn.execute(
        f"SELECT player_name, matches_played, total_score, avg_score FROM {view} ORDER BY player_name"
    ).fetchall()
    return [
        {"player": r[0], "matches": r[1], "total_score": r[2], "avg_score": r[3]}
        for r in rows
    ]


def query_score_differential(conn, mode):
    view = f"v_score_differential_{mode}"
    rows = conn.execute(
        f"SELECT differential, match_count FROM {view} ORDER BY differential"
    ).fetchall()
    return [{"differential": r[0], "match_count": r[1]} for r in rows]


API_ROUTES = {
    "/api/shooting-pct": query_shooting_pct,
    "/api/win-loss-daily": query_win_loss_daily,
    "/api/player-stats": query_player_stats,
    "/api/mvp-wins": query_mvp_wins,
    "/api/mvp-losses": query_mvp_losses,
    "/api/weekday": query_weekday,
    "/api/avg-score": query_avg_score,
    "/api/score-differential": query_score_differential,
}


def make_handler(conn):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

        def _json_response(self, data):
            body = json.dumps(data).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if path == "/api/matches":
                self._json_response(query_matches(conn, qs))
            elif path.startswith("/api/matches/") and path.endswith("/players"):
                match_id = path.split("/")[3]
                self._json_response(query_match_players(conn, int(match_id)))
            elif path in API_ROUTES:
                mode = qs.get("mode", ["3v3"])[0]
                if mode not in ALLOWED_MODES:
                    mode = "3v3"
                handler_fn = API_ROUTES[path]
                self._json_response(handler_fn(conn, mode))
            else:
                super().do_GET()

    return Handler


def main():
    import os

    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run ingest.py first.")
        raise SystemExit(1)

    conn = sqlite3.connect(DB_PATH)
    apply_migrations(conn)

    handler = make_handler(conn)
    server = HTTPServer((host, port), handler)
    print(f"Serving on http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()
        server.server_close()


if __name__ == "__main__":
    main()
