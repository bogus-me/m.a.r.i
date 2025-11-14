FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-venv python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY entrypoint.sh .
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
