FROM dhi.io/python:3-alpine-dev

# Install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Copy the application into the container.
COPY . /app

# Install the application dependencies.
WORKDIR /app
RUN uv sync --frozen --no-cache

# Run the application.
CMD ["/app/.venv/bin/fastapi", "run", "src/stadvdb_mco2/__init__.py", "--host", "0.0.0.0", "--port", "8000"]