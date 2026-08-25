"""lark-fs CLI entrypoint."""

from argparse import ArgumentParser
from asyncio import run
from os import environ
from pathlib import Path
from sys import stderr

from .reindex import reindex
from .store import Store
from .sync import ALL, sync_all
from .tui import SyncAbortedError, print_summary, run_with_tui


def build_parser():
    p = ArgumentParser(prog="lark-fs", description="Mirror Feishu/Lark into a greppable file tree.")
    p.add_argument("command", choices=["sync", "status", "reindex"], nargs="?", default="sync")
    # a fixed default keeps one store instead of scattering `lark-data/` wherever it was run
    default_root = Path(environ.get("LARK_FS_ROOT") or Path.home() / "lark-data")
    p.add_argument("--root", type=Path, default=default_root, help=f"destination directory (default: {default_root})")
    p.add_argument("--only", nargs="+", choices=ALL, help="sync only these collections")
    return p


def main():
    args = build_parser().parse_args()
    if args.command == "status":
        print_summary(Store(args.root))
        return
    if args.command == "reindex":
        counts = reindex(args.root)
        broken = f", {counts['unreadable']} unreadable" if counts["unreadable"] else ""
        print(f"  scanned {counts['messages']} messages -> {counts['users']} users, {counts['media']} media refs{broken}")
        print_summary(Store(args.root))
        return
    try:
        store = run(run_with_tui(lambda p: sync_all(args.root, p, args.only), args.only))
    except (SyncAbortedError, KeyboardInterrupt):
        # cursors are committed per slice, so an interrupted run just resumes next time
        print("\n  interrupted; rerun to resume", file=stderr)
        print_summary(Store(args.root))
        return
    print_summary(store)


if __name__ == "__main__":
    main()
