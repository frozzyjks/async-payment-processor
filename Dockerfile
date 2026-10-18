FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /usr/local/bin/uv

RUN groupadd --system app && useradd --system --gid app --home-dir /app app
WORKDIR /app

COPY pyproject.toml uv.lock ./


FROM base AS runtime
RUN uv sync --frozen --no-dev --no-install-project
COPY alembic.ini ./
COPY migrations ./migrations
COPY app ./app
USER app
EXPOSE 8000
CMD ["uvicorn", "--factory", "app.api.main:build_default_app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]


FROM base AS test
RUN uv sync --frozen --no-install-project
COPY alembic.ini ./
COPY migrations ./migrations
COPY app ./app
COPY tests ./tests
ENV PYTEST_ADDOPTS="-p no:cacheprovider"
USER app
CMD ["pytest", "-v"]
