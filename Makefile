SHELL=/bin/bash

.venv:  ## Set up virtual environment
	uv sync --all-extras --dev

install: .venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop --uv

install-release: .venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop --uv --release

pre-commit: .venv
	uv run pre-commit install
	uv run pre-commit run --all-files
	uv run mypy polars_iptools tests

clean:
	cargo clean
	find polars_iptools -name "*.so" -type f -delete

fetch-test-mmdb:
	@curl -L -o tests/maxmind/GeoLite2-City.mmdb https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoLite2-City-Test.mmdb
	@curl -L -o tests/maxmind/GeoLite2-ASN.mmdb https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoLite2-ASN-Test.mmdb

test: .venv
	uv run pytest tests

test-matrix: .venv
	uv run hatch run test:tests

run: install
	uv run run.py

run-release: install-release
	uv run run.py
