FROM python:3.12.10-bookworm AS builder

RUN pip install poetry==2.0.1

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_VIRTUALENVS_CREATE=1

WORKDIR /app

COPY pyproject.toml poetry.lock ./

# Add dummy README.md and avoid poetry warning
RUN touch README.md

RUN poetry install --no-root --no-cache --without dev

FROM python:3.12.10-slim-bookworm AS runtime

ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH"

COPY --from=builder ${VIRTUAL_ENV} ${VIRTUAL_ENV}

COPY edelweiss /opt/dagster/edelweiss

COPY data /opt/dagster/data

WORKDIR /opt/dagster

RUN adduser --disabled-password --gecos "" edelweiss

USER edelweiss

EXPOSE 4000

ENTRYPOINT ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "edelweiss"]
