"""HMR entry point: `uv run hmr.py <command>` runs any lark-fs command while you edit it.

Five things have to line up for hot reload to actually take effect here, and each one
fails silently on its own:

1. `hmr hmr.py` cannot work for this program. That CLI runs the entry file
   *synchronously* and only starts its file watcher afterwards, so an entry that blocks
   in `asyncio.run` never lets watching begin. `SyncReloaderAPI` watches on its own
   thread, so the loop below can run indefinitely.
2. The package must be imported *after* the reloader exists. Creating it patches
   `sys.meta_path`; anything imported earlier is held by the ordinary loader and is
   invisible to reloads.
3. Every call has to go through the module object. A reload rebinds names inside a
   module, so a `from lark_fs.daemon import watch` here would capture the old function
   and keep calling it forever.
4. The stop contract must not be reloadable. Rebinding applies to classes too, so a
   reloaded `SyncAbortedError` no longer catches one raised a moment earlier; `abort.py`
   is excluded from the reloader for that reason.
5. Loop-bound state must be rebuilt per cycle. Every cycle is a fresh `asyncio.run`, and
   a reload re-executes only what changed, so `cli.reset_loop_state()` reconstructs the
   semaphore and rate gate that would otherwise stay pinned to the dead loop.

State lives on disk, so an edit costs one interrupted sweep and the cursors resume it.

    uv run hmr.py sync [--root DIR] [--only ...]
    uv run hmr.py watch [--root DIR] [--interval SECONDS]
"""

from asyncio import run
from importlib import import_module
from pathlib import Path
from sys import argv, stderr

from reactivity.hmr import post_reload
from reactivity.hmr.api import SyncReloaderAPI

SRC = Path(__file__).parent / "src"
# The reloader re-executes its entry on every reload, so it must not be this file --
# `__enter__` would run hmr.py again and recurse until the stack gives out -- nor a
# package module, which breaks its relative imports when run as `__main__`. A dedicated
# empty file is the only thing that is safe to re-execute.
ENTRY = Path(__file__).parent / "_reload_entry.py"

# The stop contract is pinned, not reloaded. A reload rebinds a module's classes, so a
# reloadable `SyncAbortedError` is a new class each time and the `except` below stops
# matching exceptions raised before it -- which is exactly how a routine edit came to print
# a traceback. Excluding the file also keeps `Aborted.reason` alive across a reload, so the
# monthly-quota stop is still reported as one.
with SyncReloaderAPI(str(ENTRY), includes=[str(SRC)], excludes=[str(SRC / "lark_fs" / "abort.py")]):
    # `from lark_fs import main` would bind the package's `main()` function, not the
    # module of the same name; import_module keeps them distinct.
    abort, cli, daemon, main, store, sync, tui = (import_module(f"lark_fs.{m}") for m in ("abort", "cli", "daemon", "main", "store", "sync", "tui"))

    # A reload updates the modules, but the run in flight already built its view closures
    # and Application from the old ones. Ending the cycle is what puts the new code on
    # screen; the loop below immediately starts another.
    reloaded: list[bool] = []
    # parsed once: the command line cannot change under us, and a lambda closing over a
    # loop variable would be a live hazard for the sake of re-reading argv. Everything a
    # reload *can* change -- `sync.ALL`, `Schedule`, the syncers -- is reached as a module
    # attribute below, so it picks up new code without this being re-run.
    args = main.build_parser().parse_args(argv[1:])

    @post_reload
    def restart_on_reload():
        reloaded.append(True)
        abort.Aborted.flag = True

    # `status` and `reindex` are over before an edit could land, and neither builds a TUI
    # to reload into. Anything not named here would otherwise fall through to a full sync.
    if args.command in ("status", "reindex"):
        main.main()
        raise SystemExit

    while True:
        reloaded.clear()
        abort.Aborted.flag = False
        # each cycle is its own `asyncio.run`, and asyncio pins a Semaphore to the loop that
        # first contends on it. A reload re-executes only the files that changed, so cli.py's
        # gate and semaphore otherwise stay bound to the loop that just died.
        cli.reset_loop_state()
        try:
            if args.command == "watch":
                run(tui.run_with_tui(lambda p: daemon.watch(args.root, p, daemon.Schedule(messages=args.interval)), [*sync.ALL, "recheck", "daemon"]))
            else:
                run(tui.run_with_tui(lambda p: sync.sync_all(args.root, p, args.only), args.only))
        except KeyboardInterrupt:
            break
        except abort.SyncAbortedError:
            # a reason means no rerun can clear this (the monthly quota), so it wins even over a
            # reload: another cycle would clear the flag and spend against the exhausted tenant,
            # and "rerun to resume" is the one answer that cannot work
            if reason := abort.Aborted.reason:
                print(f"\n  stopped: {reason}", file=stderr)
                break
            # only the reload hook tells a restart from a ctrl-c; guessing makes the app unquittable
            if not reloaded:
                print("\n  interrupted; rerun to resume", file=stderr)
                break
        else:
            break  # a one-shot command that ran to completion; only a reload restarts it

    print(file=stderr)
    tui.print_summary(store.Store(args.root))
