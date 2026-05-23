FROM ghcr.io/astral-sh/uv:python3.14-trixie-slim

ARG RRROCKET_VERSION=0.11.1

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
  && curl -fsSL "https://github.com/nickbabcock/rrrocket/releases/download/v${RRROCKET_VERSION}/rrrocket-${RRROCKET_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
  | tar xz --strip-components=1 -C /usr/local/bin \
  && chmod +x /usr/local/bin/rrrocket \
  && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/* \
  && groupadd -g 1000 appuser && useradd -u 1000 -g appuser -m appuser \
  && mkdir -p /app/db /app/replays && chown appuser:appuser /app /app/db /app/replays

WORKDIR /app
USER appuser

COPY --chown=appuser:appuser pyproject.toml uv.lock ./
RUN uv sync --locked --no-editable --compile-bytecode --no-dev --no-install-project --no-cache

COPY --chown=appuser:appuser server.py ingest.py db.py process.py frame_analysis.py player_identity.py config.py rrrocket_schema.py ./
COPY --chown=appuser:appuser migrations/ migrations/
COPY --chown=appuser:appuser sql/ sql/
COPY --chown=appuser:appuser static/ static/

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

EXPOSE 8080

CMD ["/app/.venv/bin/python", "server.py"]
