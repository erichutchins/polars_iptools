SHELL=/bin/bash

.venv:  ## Set up virtual environment
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

install: .venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop

install-release: .venv
	unset CONDA_PREFIX && \
	source .venv/bin/activate && maturin develop --release

pre-commit: .venv
	cargo fmt --all && cargo clippy --all-features
	.venv/bin/python -m ruff check . --fix --exit-non-zero-on-fix
	.venv/bin/python -m ruff check --select I . --fix --exit-non-zero-on-fix
	.venv/bin/python -m ruff format polars_iptools tests
	.venv/bin/mypy polars_iptools tests

clean:
	cargo clean
	find polars_iptools -name "*.so" -type f -delete

fetch-test-mmdb:
	@curl -L -o tests/maxmind/GeoLite2-City.mmdb https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoLite2-City-Test.mmdb
	@curl -L -o tests/maxmind/GeoLite2-ASN.mmdb https://raw.githubusercontent.com/maxmind/MaxMind-DB/main/test-data/GeoLite2-ASN-Test.mmdb

test: .venv
	.venv/bin/python -m pytest tests

run: install
	source .venv/bin/activate && python run.py

run-release: install-release
	source .venv/bin/activate && python run.py
