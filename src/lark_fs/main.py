"""lark-fs CLI entrypoint."""

from argparse import ArgumentParser
from asyncio import run
from os import environ
from pathlib import Path
from signal import SIGINT, SIGTERM, signal
from sys import stderr

from .abort import Aborted, SyncAbortedError
from .cli import session_scope
from .daemon import Schedule, watch
from .reindex import reindex
from .store import Store
from .sync import ALL, sync_all, wanted
from .tui import print_summary, run_with_tui


def build_parser():
    p = ArgumentParser(prog="lark-fs", description="Mirror Feishu/Lark into a greppable file tree.")
    p.add_argument("command", choices=["sync", "status", "reindex", "watch"], nargs="?", default="sync")
    default_root = Path(environ.get("LARK_FS_ROOT") or "lark-data")
    p.add_argument("--root", type=Path, default=default_root, help=f"destination directory (default: {default_root})")
    p.add_argument("--only", nargs="+", choices=ALL, help="sync only these collections")
    p.add_argument("--skip", nargs="+", choices=ALL, help="leave these collections alone; a skipped pass is no longer swept")
    p.add_argument("--interval", type=float, default=120, help="watch: seconds between message sweeps")
    return p


def _install_stop_handler():
    """Without a TTY there is no key binding, so SIGINT/SIGTERM set the stop flag instead."""

    def stop(*_):
        Aborted.flag = True

    for sig in (SIGINT, SIGTERM):
        signal(sig, stop)


def main():
    args = build_parser().parse_args()
    _install_stop_handler()
    if args.command == "status":
        print_summary(Store(args.root))
        return
    if args.command == "reindex":
        counts = reindex(args.root)
        broken = f", {counts['unreadable']} unreadable" if counts["unreadable"] else ""
        moved = f"  split {counts['threads_split']} threads into one file per message\n" if counts["threads_split"] else ""
        print(f"{moved}  scanned {counts['messages']} messages -> {counts['users']} users, {counts['media']} media refs, {counts['doc_links']} linked docs{broken}")
        print_summary(Store(args.root))
        return
    if args.command == "watch":
        rows = [*ALL, "recheck", "daemon"]
        try:
            run(session_scope(run_with_tui(lambda p: watch(args.root, p, Schedule(messages=args.interval)), rows)))
        except SyncAbortedError, KeyboardInterrupt:
            print(f"\n  stopped: {Aborted.reason}" if Aborted.reason else "\n  stopped", file=stderr)
        print(file=stderr)
        print_summary(Store(args.root))
        return

    try:
        # The drawn rows are not the flags the sync itself reads: `only` is what tells the
        # periodic passes they may skip a turn, so it has to reach `sync_all` untouched.
        store = run(session_scope(run_with_tui(lambda p: sync_all(args.root, p, args.only, args.skip), wanted(args.only, args.skip))))
    except SyncAbortedError, KeyboardInterrupt:
        # cursors are committed per slice, so an interrupted run just resumes next time --
        # unless it stopped for a reason rerunning cannot clear, which only the stop knows
        print(f"\n  stopped: {Aborted.reason}" if Aborted.reason else "\n  interrupted; rerun to resume", file=stderr)
        print_summary(Store(args.root))
        return
    print(file=stderr)
    print_summary(store)


if __name__ == "__main__":
    main()
