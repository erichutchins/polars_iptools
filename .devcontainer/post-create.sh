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

step "Volume permissions" "$(dirname "$0")/fix-volume-perms.sh"

# rust-toolchain.toml drives the version; this materialises it up front so the
# first cargo invocation isn't a surprise download.
step "Rust toolchain" rustup show active-toolchain
step "Rust components" rustup component add clippy rustfmt

# The base image ships its own default toolchain and a pinned nightly, neither
# of which this project uses -- rust-toolchain.toml selects stable. Three
# toolchains is ~2GB of a disk budget that a polars build needs for artifacts.
prune_toolchains() {
    local active
    active=$(rustup show active-toolchain | head -1 | cut -d' ' -f1) || return 1
    [ -n "$active" ] || return 1
    # Toolchains baked into the image are owned by root, so rustup cannot remove
    # them as vscode. Hand the tree over first; this also lets later
    # `rustup update`/`component add` work without sudo.
    sudo chown -R "$(id -u):$(id -g)" "${RUSTUP_HOME:-$HOME/.rustup}/toolchains" || true
    rustup default "$active" || return 1
    rustup toolchain list | awk '{print $1}' | grep -vx "$active" |
        while read -r tc; do
            echo "    removing unused toolchain: $tc"
            rustup toolchain uninstall "$tc" || true
        done
}
step "Pruning unused Rust toolchains" prune_toolchains

# remoteEnv covers interactive shells, but this script runs before that applies.
export PATH="${HOME}/.local/bin:${PATH}"

# just is the entry point that bootstraps everything else, so it has to exist
# before the project venv does. maturin, prek, hatch and the rest are project
# dev dependencies -- they come from `uv sync` below and are run via `uv run`,
# so installing them as global tools too would just be a second copy free to
# drift from the pinned one.
step "Installing just" uv tool install rust-just

# --no-install-project deliberately skips compiling the crate. Building it here
# would mean a full release build (lto = "fat", codegen-units = 1) during
# provisioning, and a compile error would take the whole container down with it.
# Use `just install` for a debug build once the container is up.
# The interpreter itself is fetched by uv, pinned by .python-version.
step "Python environment" \
    uv sync --all-extras --dev --group docs --no-install-project

step "Git hooks" uv run prek install

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
