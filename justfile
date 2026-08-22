set shell := ["bash", "-c"]

_default:
    @just --list

# Set up virtual environment
setup:
    uv sync --all-extras --dev --group docs

# Install the package in development mode
install: setup
    uv run maturin develop --uv

# Install the package in release mode
install-release: setup
    uv run maturin develop --uv --release

# Install prek git hooks
hooks: setup
    uv run prek install

# Run all prek hooks
lint: setup
    uv run prek run --all-files

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
test-matrix: setup
    uv run hatch run test:tests

# Run tests for a specific python version (e.g. 3.12)
test-version version: setup
    uv run hatch run +py={{version}} test:tests

# Run the example script
run: install
    uv run run.py

# Run the example script in release mode
run-release: install-release
    uv run run.py

# Test mkdocs locally
docs-serve:
    uv run --group docs mkdocs serve
