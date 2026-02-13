FROM python:3.14-slim

ARG RRROCKET_VERSION=0.10.11

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
    && curl -fsSL "https://github.com/nickbabcock/rrrocket/releases/download/v${RRROCKET_VERSION}/rrrocket-${RRROCKET_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
       | tar xz --strip-components=1 -C /usr/local/bin \
    && chmod +x /usr/local/bin/rrrocket \
    && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY server.py ingest.py db.py ./
COPY migrations/ migrations/
COPY static/ static/
COPY scripts/ scripts/

RUN mkdir -p db replays

EXPOSE 8080

CMD ["python", "server.py"]
