"""HMR entry point: `hmr watch.py` keeps the mirror fresh while you edit the code.

hmr reloads by re-executing this file's top level, so the daemon loop has to live here
rather than inside something that outlives the reload -- and it has to yield when a
reload is pending, or hmr would sit waiting for a loop that never ends. `pre_reload`
raises the same cooperative stop that ctrl-c uses, and re-entry starts the new code.

State lives on disk, which is exactly what a mirror wants: an edit costs one interrupted
sweep, and the cursors mean the next pass picks up where that one stopped.

    hmr watch.py [--root DIR] [--interval SECONDS]
"""

from asyncio import run
from sys import argv

from reactivity.hmr import pre_reload

from lark_fs.cli import Aborted, SyncAbortedError
from lark_fs.daemon import Schedule, watch
from lark_fs.main import build_parser
from lark_fs.sync import ALL
from lark_fs.tui import run_with_tui


@pre_reload
def stop_for_reload():
    Aborted.flag = True


args = build_parser().parse_args(["watch", *argv[1:]])
Aborted.flag = False  # clear the stop the previous generation exited on

try:
    run(run_with_tui(lambda p: watch(args.root, p, Schedule(messages=args.interval)), [*ALL, "recheck", "daemon"]))
except SyncAbortedError, KeyboardInterrupt:
    pass
