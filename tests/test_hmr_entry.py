"""Run the production HMR entry with real reloads and a local workload, without Lark requests."""

from pathlib import Path
from shutil import copy2, copytree, ignore_patterns
from subprocess import run
from sys import executable

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Only the daemon's workload is substituted. The entry, reload configuration, TUI,
# exception handler and loop reset all run from the production files in an isolated copy.
PROBE = """
import asyncio
import sys
from importlib import import_module
from pathlib import Path
from runpy import run_path

from prompt_toolkit.application import create_app_session
from prompt_toolkit.input import create_pipe_input
from prompt_toolkit.output import DummyOutput
from reactivity.hmr import post_reload
from reactivity.hmr import api

root = Path(__file__).parent
edited, mode = sys.argv[1:]
cycles, reloads, stamps = [], [], []
held = []
modules = {}

class Terminal:
    def __init__(self, wrapped):
        self.wrapped = wrapped

    def __getattr__(self, name):
        return getattr(self.wrapped, name)

    def isatty(self):
        return True

async def contend(cli):
    async def slot():
        async with cli._sem:
            await asyncio.sleep(0)
    await asyncio.gather(*(slot() for _ in range(40)))
    # Force the rate gate's lock to bind too; a search under its budget never suspends.
    async with cli.search_gate.lock:
        waiter = asyncio.create_task(cli.search_gate.__aenter__())
        await asyncio.sleep(0)
    await waiter

async def workload(store_root, progress, schedule):
    cli, abort = modules['cli'], modules['abort']
    cycles.append(asyncio.get_running_loop())
    print('cycle:', len(cycles), flush=True)
    if len(cycles) == 2:
        assert mode == 'resume', 'a terminal stop restarted the workload'
        assert cycles[0] is not cycles[1], 'the second cycle must use a new event loop'
        assert list(cli.search_gate.stamps) == stamps, 'reload erased the rolling search budget'
        assert type(cli.search_gate) is cli.RateGate, 'reload kept the old rate gate implementation'
        await contend(cli)
        print('resumed with fresh locks and preserved budget', flush=True)
        return modules['store'].Store(store_root)

    held.append(abort.SyncAbortedError('raised before reload'))
    await contend(cli)
    stamps.extend(cli.search_gate.stamps)
    old_sem, old_gate = cli._sem, cli.search_gate
    for _ in range(cli.CONCURRENCY):
        await old_sem.acquire()
    if mode == 'quota':
        abort.Aborted.reason = 'monthly quota'
    target = root / 'src' / 'lark_fs' / edited
    target.write_text(target.read_text() + '\\n# entry regression reload\\n')
    try:
        async with asyncio.timeout(10):
            while not abort.Aborted.flag:
                # Retry the notification until the watcher observes it, including slow startup.
                target.touch()
                await asyncio.sleep(0.1)
        assert reloads, 'the production reload hook did not run'
        assert isinstance(held[0], abort.SyncAbortedError), 'reload replaced the abort contract'
        waiter = asyncio.create_task(cli._sem.acquire())
        try:
            await asyncio.sleep(0)
            assert not waiter.done(), 'reload admitted a ninth request while eight still hold slots'
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
        assert cli.search_gate is old_gate, 'reload replaced the gate while its old waiters are still active'
    finally:
        for _ in range(cli.CONCURRENCY):
            old_sem.release()
    print('reload observed', flush=True)
    if mode == 'interrupt':
        pipe.send_text('\\x03')
        await asyncio.sleep(10)
        raise AssertionError('the TUI ignored Ctrl-C')
    raise held[0]

class ProbeReloader(api.SyncReloaderAPI):
    def __enter__(self):
        result = super().__enter__()
        modules.update((name, import_module('lark_fs.' + name)) for name in ('cli', 'abort', 'daemon', 'store'))
        install_workload()
        return result

def install_workload():
    modules['daemon'].watch = workload

@post_reload
def observe_reload():
    # the initial load calls the post-reload hooks too, before ProbeReloader.__enter__ has
    # populated `modules` -- this is a real guard, not dead code
    if modules:
        install_workload()
        reloads.append(True)

api.SyncReloaderAPI = ProbeReloader
sys.stderr = Terminal(sys.stderr)
sys.path.insert(0, str(root / 'src'))
sys.argv = [str(root / 'hmr.py'), 'watch', '--root', str(root / 'data')]
with create_pipe_input() as pipe, create_app_session(input=pipe, output=DummyOutput()):
    run_path(str(root / 'hmr.py'), run_name='__main__')
assert len(cycles) == (2 if mode == 'resume' else 1), cycles
if mode == 'quota':
    assert modules['abort'].Aborted.flag, 'the terminal stop was cleared'
    assert modules['abort'].Aborted.reason == 'monthly quota', 'the quota reason was lost'
print('entry completed', flush=True)
"""


@pytest.mark.parametrize(("edited", "mode"), [("tui.py", "resume"), ("cli.py", "resume"), ("abort.py", "resume"), ("cli.py", "quota"), ("cli.py", "interrupt")])
def test_hmr_entry(tmp_path, edited, mode):
    copytree(ROOT / "src", tmp_path / "src", ignore=ignore_patterns("__pycache__"))
    for name in ("hmr.py", "_reload_entry.py"):
        copy2(ROOT / name, tmp_path / name)
    (tmp_path / "data" / "chats").mkdir(parents=True)
    probe = tmp_path / "probe.py"
    probe.write_text(PROBE)
    result = run([executable, str(probe), edited, mode], cwd=tmp_path, capture_output=True, text=True, timeout=30, check=False)
    assert "cycle: 1" in result.stdout, result.stdout + result.stderr
    assert "reload observed" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0, result.stdout + result.stderr
    assert "entry completed" in result.stdout, result.stdout + result.stderr
    assert "Traceback" not in result.stderr, result.stderr
    if mode == "quota":
        assert "stopped: monthly quota" in result.stderr, result.stderr
    if mode == "resume":
        assert "resumed with fresh locks and preserved budget" in result.stdout
