# Polars IPTools

Polars IPTools is a Rust-based extension to accelerate IP address manipulation and enrichment in [Polars](https://pola.rs/) dataframes. This library includes various utility functions for working with IPv4 and IPv6 addresses and geoip and anonymization/proxy enrichment using [MaxMind](https://www.maxmind.com/) databases.

## Install

```shell
pip install polars-iptools
# or
uv add polars-iptools
```

## Credit

Developing this extension was super easy by following Marco Gorelli's [tutorial](https://marcogorelli.github.io/polars-plugins-tutorial/) and [cookiecutter template](https://github.com/MarcoGorelli/cookiecutter-polars-plugins).

## Development

This project uses `just` for managing development tasks.

### Install Just

You can install `just` using Homebrew or `uv`:

```shell
brew install just
# or
uv tool install rust-just
```

### Usage

```shell
just setup          # Set up virtual environment
just install        # Install package in dev mode
just test           # Run tests
just test-matrix    # Run tests across all python versions
just --list         # List all available commands
```
