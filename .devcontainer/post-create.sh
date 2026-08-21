#!/usr/bin/env bash
# Provision the container after creation. Idempotent: safe to re-run.
#
# This script always exits 0. Provisioning must never leave the user without a
# container, because the container is where they go to fix whatever went wrong.
# A failed step warns and continues; the banner at the end reports the damage.
set -uo pipefail

WARNINGS=0
warn() {
    echo "WARNING: $*" >&2
    WARNINGS=$((WARNINGS + 1))
}
step() {
    echo "==> $1"
    shift
    "$@" || warn "'$*' failed."
}

# Named volumes mount as root; the toolchain runs as vscode.
step "Taking ownership of cache volumes" \
    sudo chown -R "$(id -u):$(id -g)" \
    /usr/local/cargo/registry \
    "${PWD}/target" \
    "${HOME}/.claude"

# rust-toolchain.toml drives the version; this materialises it up front so the
# first cargo invocation isn't a surprise download.
step "Rust toolchain" rustup show active-toolchain
step "Rust components" rustup component add clippy rustfmt

# remoteEnv covers interactive shells, but this script runs before that applies.
export PATH="${HOME}/.local/bin:${PATH}"
step "Installing just" uv tool install rust-just
step "Installing prek" uv tool install prek
step "Installing maturin" uv tool install maturin

# --no-install-project deliberately skips compiling the crate. Building it here
# would mean a full release build (lto = "fat", codegen-units = 1) during
# provisioning, and a compile error would take the whole container down with it.
# Use `just install` for a debug build once the container is up.
step "Python environment" \
    uv sync --all-extras --dev --group docs --no-install-project

step "Git hooks" prek install

cat <<'BANNER'

polars_iptools devcontainer ready.

  just install    build the extension in debug mode (do this first)
  just test       run the pytest suite
  just lint       run prek across all files
  just --list     everything else
  claude          start Claude Code (first run prompts for login)

BANNER

if [ "$WARNINGS" -gt 0 ]; then
    echo "Provisioning finished with ${WARNINGS} warning(s) — see above." >&2
fi

# Always succeed: see the note at the top of this script.
exit 0
