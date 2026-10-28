#!/usr/bin/env python3
"""Container entrypoint: prepare the SQLite directory, drop privileges, exec.

The image is started as root *only* to make the bind-mounted data directory
(``./data:/app/data``) writable by the unprivileged ``appuser`` — Docker
creates a missing host directory as ``root:root 0755`` before the container
starts, which would otherwise make ``sqlite3`` fail with
``unable to open database file`` and crash-loop the egress worker.

After the ownership fix the process identity becomes ``appuser`` and the real
service command is ``exec``-ed (so it keeps PID 1 and receives SIGTERM/SIGINT
for a graceful shutdown).
"""

from __future__ import annotations

import os
import pwd
import sys
from pathlib import Path

DATA_DIR = Path(os.environ.get("GATEWAY_DATA_DIR", "/app/data"))
TARGET_USER = os.environ.get("GATEWAY_APP_USER", "appuser")


def _chown_tree(path: Path, uid: int, gid: int) -> None:
    """Best-effort recursive chown of the data directory."""
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            try:
                os.chown(os.path.join(root, name), uid, gid)
            except OSError as exc:  # pragma: no cover - depends on host mount
                print(f"entrypoint: chown {root}/{name} failed: {exc}", file=sys.stderr)


def prepare_data_dir(path: Path, uid: int, gid: int) -> None:
    """Make sure the shared SQLite directory exists and is writable by uid/gid.

    The directory is opened with 0777 so the host operator keeps read/write
    access for backups (``sqlite3 ./data/gateway.db '.backup ...'``) even
    though the container owns it.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
        _chown_tree(path, uid, gid)
        os.chown(path, uid, gid)
        os.chmod(path, 0o777)
    except OSError as exc:
        print(f"entrypoint: cannot prepare {path}: {exc}", file=sys.stderr)


def drop_privileges(user: str) -> pwd.struct_passwd:
    entry = pwd.getpwnam(user)
    os.setgroups([entry.pw_gid])
    os.setgid(entry.pw_gid)
    os.setuid(entry.pw_uid)
    os.environ["HOME"] = entry.pw_dir or "/app"
    return entry


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: docker_entrypoint.py <command> [args...]", file=sys.stderr)
        return 2

    if os.geteuid() == 0:
        entry = pwd.getpwnam(TARGET_USER)
        prepare_data_dir(DATA_DIR, entry.pw_uid, entry.pw_gid)
        drop_privileges(TARGET_USER)
        print(
            f"entrypoint: prepared {DATA_DIR} for {TARGET_USER} "
            f"(uid={os.geteuid()}, gid={os.getegid()})",
            file=sys.stderr,
        )
    else:
        # Already running unprivileged (e.g. `docker run --user appuser`):
        # just try to keep the directory usable, never fail hard here.
        entry = pwd.getpwuid(os.geteuid())
        prepare_data_dir(DATA_DIR, entry.pw_uid, entry.pw_gid)

    try:
        os.execvp(argv[0], argv)
    except FileNotFoundError:
        print(f"entrypoint: command not found: {argv[0]}", file=sys.stderr)
        return 127
    return 0  # pragma: no cover - execvp never returns on success


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
