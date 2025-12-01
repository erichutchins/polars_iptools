set shell := ["bash", "-c"]

_default:
    @just --list

# Set up virtual environment
setup:
    uv sync --all-extras --dev --optional docs

# Ensure maturin is available; install via uv if missing
require-maturin:
    if ! command -v maturin >/dev/null 2>&1; then \
        echo "maturin not found — installing via uv"; \
        uv tool install maturin; \
    else \
        echo "maturin found"; \
    fi

# Ensure hatch is available; install via uv if missing
require-hatch:
    if ! command -v hatch >/dev/null 2>&1; then \
        echo "hatch not found — installing via uv"; \
        uv tool install hatch; \
    else \
        echo "hatch found"; \
    fi

# Install the package in development mode
install: setup require-maturin
    unset CONDA_PREFIX && source .venv/bin/activate && maturin develop --uv

# Install the package in release mode
install-release: setup require-maturin
    unset CONDA_PREFIX && source .venv/bin/activate && maturin develop --uv --release

# Run pre-commit checks
pre-commit: setup
    uv run pre-commit install
    uv run pre-commit run --all-files
    uv run mypy polars_iptools tests

# Clean up build artifacts
clean:
    cargo clean
    find polars_iptools -name "*.so" -type f -delete

# Fetch test MMDB files
fetch-test-mmdb:
    curl -L -o tests/maxmind/GeoLite2-City.mmdb https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoLite2-City-Test.mmdb
    curl -L -o tests/maxmind/GeoLite2-ASN.mmdb https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoLite2-ASN-Test.mmdb

# Run tests
test: setup
    uv run pytest tests

# Run tests across all supported Python versions
test-matrix: setup require-hatch
    hatch run test:tests

# Run tests for a specific python version (e.g. 3.12)
test-version version: setup require-hatch
    hatch run +py={{version}} test:tests

# Run the example script
run: install
    uv run run.py

# Run the example script in release mode
run-release: install-release
    uv run run.py

# Test mkdocs locally
docs-serve:
    uv run --group docs mkdocs serve