# Producer and consumer image (decision D23): the capture runs as docker compose services,
# not in an application's terminal, so app updates and closed windows cannot kill it.
FROM python:3.12.14-slim-trixie

ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

RUN pip install --no-cache-dir uv==0.12.7

WORKDIR /app
# Dependencies first (cached layer), then the project. --no-dev leaves out PySpark and the
# test tools; the package stays editable so contract.py finds /app/schemas/*.avsc.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY schemas ./schemas
RUN uv sync --frozen --no-dev

# Same uid as the host user, so the read-only ~/.aws mount (mode 600) is readable.
RUN useradd --uid 501 --create-home app
USER app
