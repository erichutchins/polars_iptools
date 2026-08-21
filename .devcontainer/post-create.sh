#!/usr/bin/env bash
# Provision the container after creation. Idempotent: safe to re-run.
set -euo pipefail

echo "==> Taking ownership of cache volumes"
# Named volumes mount as root; the toolchain runs as vscode.
sudo chown -R "$(id -u):$(id -g)" \
    /usr/local/cargo/registry \
    "${PWD}/target" \
    "${HOME}/.claude"

echo "==> Rust toolchain"
# rust-toolchain.toml drives the version; this materialises it up front so the
# first cargo invocation isn't a surprise download.
rustup show active-toolchain
rustup component add clippy rustfmt

echo "==> Developer tools"
# remoteEnv covers interactive shells, but this script runs before that applies.
export PATH="${HOME}/.local/bin:${PATH}"
uv tool install rust-just
uv tool install prek
uv tool install maturin

echo "==> Python environment"
just setup

echo "==> Git hooks"
prek install

cat <<'BANNER'

polars_iptools devcontainer ready.

  just install    build the extension in debug mode
  just test       run the pytest suite
  just lint       run prek across all files
  just --list     everything else
  claude          start Claude Code (first run prompts for login)

BANNER
