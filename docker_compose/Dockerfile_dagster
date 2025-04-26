FROM python:3.12.9-slim-bookworm

RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-duckdb \
    dagster-docker

# Set $DAGSTER_HOME and copy dagster.yaml and workspace.yaml there
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME

COPY dagster.yaml workspace.yaml $DAGSTER_HOME

WORKDIR $DAGSTER_HOME
