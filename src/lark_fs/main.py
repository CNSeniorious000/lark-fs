"""lark-fs CLI entrypoint."""

from argparse import ArgumentParser
from asyncio import run
from pathlib import Path

from .store import Store
from .sync import ALL, sync_all
from .tui import print_summary, run_with_tui


def build_parser():
    p = ArgumentParser(prog="lark-fs", description="Mirror Feishu/Lark into a greppable file tree.")
    p.add_argument("command", choices=["sync", "status"], nargs="?", default="sync")
    p.add_argument("--root", type=Path, default=Path.cwd() / "lark-data", help="destination directory")
    p.add_argument("--only", nargs="+", choices=ALL, help="sync only these collections")
    return p


def main():
    args = build_parser().parse_args()
    if args.command == "status":
        print_summary(Store(args.root))
        return
    store = run(run_with_tui(lambda p: sync_all(args.root, p, args.only), args.only))
    print_summary(store)


if __name__ == "__main__":
    main()
