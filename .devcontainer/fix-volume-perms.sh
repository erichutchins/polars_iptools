#!/usr/bin/env bash
# Named volumes mount as root; the toolchain runs as vscode.
#
# This runs from both onCreateCommand (via post-create.sh) and postStartCommand.
# The volumes outlive the container, so a volume recreated after the container
# already exists -- `docker volume rm`, or a fresh volume on another machine --
# would otherwise remount as root with nothing left to fix it. The -O guard
# makes the repeat case a no-op instead of a recursive chown over ~1GB.
set -uo pipefail

for dir in /usr/local/cargo/registry "${PWD}/target" "${HOME}/.claude"; do
    [ -d "$dir" ] || continue
    [ -O "$dir" ] && continue
    sudo chown -R "$(id -u):$(id -g)" "$dir" || echo "WARNING: could not chown $dir" >&2
done

exit 0
