import json
import sqlite3
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

from db import apply_migrations

DB_PATH = Path("db/rl_stats.sqlite")
STATIC_DIR = Path(__file__).parent / "static"


def query_shooting_pct(conn):
    rows = conn.execute(
        "SELECT player_name, total_goals, total_shots, shooting_pct FROM v_shooting_pct ORDER BY player_name"
    ).fetchall()
    return [
        {"player": r[0], "goals": r[1], "shots": r[2], "shooting_pct": r[3]}
        for r in rows
    ]


def query_win_loss_daily(conn):
    rows = conn.execute(
        "SELECT play_date, wins, losses, win_rate FROM v_win_loss_daily"
    ).fetchall()
    return [
        {"date": r[0], "wins": r[1], "losses": r[2], "win_rate": r[3]} for r in rows
    ]


def query_player_stats(conn):
    rows = conn.execute(
        "SELECT player_name, matches_played, total_goals, total_assists, total_saves, total_shots FROM v_player_stats ORDER BY player_name"
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


def query_mvp_wins(conn):
    rows = conn.execute(
        "SELECT player_name, mvp_matches, mvp_wins, mvp_win_rate FROM v_mvp_win_rate ORDER BY player_name"
    ).fetchall()
    return [
        {"player": r[0], "mvp_matches": r[1], "mvp_wins": r[2], "win_rate": r[3]}
        for r in rows
    ]


def query_mvp_losses(conn):
    rows = conn.execute(
        "SELECT player_name, loss_mvps FROM v_mvp_in_losses ORDER BY player_name"
    ).fetchall()
    return [{"player": r[0], "loss_mvps": r[1]} for r in rows]


def query_weekday(conn):
    rows = conn.execute(
        "SELECT weekday, matches_played, wins, losses, win_rate FROM v_win_loss_by_weekday"
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


API_ROUTES = {
    "/api/shooting-pct": query_shooting_pct,
    "/api/win-loss-daily": query_win_loss_daily,
    "/api/player-stats": query_player_stats,
    "/api/mvp-wins": query_mvp_wins,
    "/api/mvp-losses": query_mvp_losses,
    "/api/weekday": query_weekday,
}


def make_handler(conn):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

        def do_GET(self):
            if self.path in API_ROUTES:
                handler_fn = API_ROUTES[self.path]
                data = handler_fn(conn)
                body = json.dumps(data).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                super().do_GET()

    return Handler


def main():
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Run ingest.py first.")
        raise SystemExit(1)

    conn = sqlite3.connect(DB_PATH)
    apply_migrations(conn)

    handler = make_handler(conn)
    server = HTTPServer(("localhost", 8080), handler)
    print("Serving on http://localhost:8080")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()
        server.server_close()


if __name__ == "__main__":
    main()
