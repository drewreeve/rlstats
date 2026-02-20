# RL Stats

Small personal project to track the stats of 3 absolute potatoes. Not really
designed for anyone else to use (players are currently hard coded) but if you
really wanted to you could. It's pretty simple really:

* Replay files are parsed by [rrrocket](https://github.com/nickbabcock/rrrocket) into JSON
* JSON gets ingested into SQLite
* Flask frontend to show it all off with some charts courtesy of
  [Chart.js](https://www.chartjs.org/)

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `SECRET_KEY` | Flask session secret key. Set this in production for stable sessions across restarts. | Random hex token (generated at startup) |
| `UPLOAD_PASSWORD` | Password required to upload replay files. Uploads are disabled if not set. | *(none)* |
| `HOST` | Address the server binds to. | `0.0.0.0` |
| `PORT` | Port the server listens on. | `8080` |

