# Single image for all three Dagster services (webserver, daemon, code-server).
# `uv sync --no-dev` -> no `dg` CLI / questionary / pytest / black, but keeps
# dagster-webserver (it's a real dependency for this deployment).

FROM python:3.11-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    DAGSTER_HOME=/opt/dagster/home

# uv, pinned for reproducible builds (keep in step with the dev toolchain)
COPY --from=ghcr.io/astral-sh/uv:0.12.7 /uv /bin/uv

WORKDIR /app

# 1) dependency layer — changes only when pyproject.toml / uv.lock change
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

# 2) app layer
COPY src ./src
COPY deploy ./deploy
RUN uv sync --frozen --no-dev

# 3) instance config + scratch dirs
RUN mkdir -p "$DAGSTER_HOME/local/compute-logs" "$DAGSTER_HOME/local/artifacts" \
 && cp deploy/prod/dagster.yaml "$DAGSTER_HOME/dagster.yaml" \
 && cp deploy/prod/workspace.yaml "$DAGSTER_HOME/workspace.yaml" \
 && cp deploy/entrypoint.sh /usr/local/bin/entrypoint.sh \
 && chmod +x /usr/local/bin/entrypoint.sh

# Geo stack sanity — fail the build if a wheel is missing its native libs
RUN python -c "import geopandas, pyogrio, pyproj, shapely, dagster_aws.s3, dagster_slack; \
    pyogrio.read_info; print('deps ok')"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
# Overridden per service in compose.prod.yaml; default = the code-location server.
CMD ["dagster", "code-server", "start", "-m", "wprdc_etl.definitions", \
     "--host", "0.0.0.0", "--port", "4000"]
