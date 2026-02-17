FROM python:3.14-slim

ARG RRROCKET_VERSION=0.10.11

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
  && curl -fsSL "https://github.com/nickbabcock/rrrocket/releases/download/v${RRROCKET_VERSION}/rrrocket-${RRROCKET_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
  | tar xz --strip-components=1 -C /usr/local/bin \
  && chmod +x /usr/local/bin/rrrocket \
  && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 appuser && useradd -u 1000 -g appuser -m appuser

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-editable --compile-bytecode --no-dev --no-install-project

COPY server.py ingest.py db.py ./
COPY migrations/ migrations/
COPY static/ static/
COPY scripts/ scripts/
RUN mkdir -p db replays && chown -R appuser:appuser /app

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  UV_CACHE_DIR=/tmp/.uv-cache

EXPOSE 8080

USER appuser

CMD ["/app/.venv/bin/python", "server.py"]
