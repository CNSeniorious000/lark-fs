"""pnpm-style inline progress: a few lines redrawn in place, no alternate screen."""

from asyncio import Event, Future, create_task, gather, sleep
from contextlib import suppress
from itertools import cycle
from sys import stderr
from xml.sax.saxutils import escape

from prompt_toolkit.application import Application, get_app
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.output import ColorDepth
from prompt_toolkit.styles import Style
from reactivity import derived, effect, signal

from .cli import CONCURRENCY, SyncAbortedError, in_flight
from .sync import ALL, Progress

SPINNER = cycle("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
GLYPH = {"pending": "<dim>·</dim>", "running": "", "done": "<ok>✓</ok>", "error": "<err>✗</err>"}
STYLE = Style.from_dict({"ok": "#22c55e", "err": "#ef4444", "dim": "#6b7280", "name": "bold", "num": "#eab308"})


def _line(row: dict, frame: str, width: int) -> str:
    mark = GLYPH[row["state"]] or f"<num>{frame}</num>"
    done, total = row["done"], row["total"]
    count = f"{done}/{total}" if total is not None else str(done)
    head = f"{mark} <name>{row['name']:<9}</name> <num>{count:<7}</num>"
    # show what was just written; fall back to the phase note when nothing has landed yet
    tail = row["last"] if row["state"] == "running" and row["last"] else row["note"]
    room = max(0, width - len(row["name"]) - len(count) - 14)
    return head + (f" <dim>{escape(tail[:room])}</dim>" if tail else "")


def _fetching(width: int) -> list[str]:
    """bun-style live view of what is on the wire right now, newest last."""
    labels = list(in_flight.values())
    return [f"  <dim>{escape(label[: width - 4])}</dim>" for label in labels]


async def run_plain(coro_factory, names: list[str] | None = None):
    """Non-TTY fallback: one line per state change, so pipes and CI still see progress."""
    progress = Progress()
    seen: set[str] = set()

    @effect
    def _report():
        for name in names or ALL:
            row = progress.rows.get(name)
            if row and row["state"] in ("done", "error") and name not in seen:
                seen.add(name)
                print(f"  {row['state']:<7} {row['name']:<9} {row['done']}  {row['note']}", file=stderr, flush=True)

    try:
        return await coro_factory(progress)
    finally:
        _report.dispose()


async def run_with_tui(coro_factory, names: list[str] | None = None):
    """Render live progress inline while `coro_factory(progress)` runs."""
    if not stderr.isatty():
        return await run_plain(coro_factory, names)

    rows = names or ALL
    app_started = Event()
    never = Future()
    progress = Progress()
    for name in rows:
        progress.set(name, state="pending")

    frame = signal(next(SPINNER))

    @derived
    def view():
        """Tracks progress.rows and the spinner, so the effect below knows when to redraw."""
        width = get_app().output.get_size().columns
        lines = [_line(progress.rows[n], frame.get(), width) for n in rows if n in progress.rows]
        return HTML("\n".join(lines + _fetching(width)))

    # height must cover the collection rows plus every concurrent request line
    body = Window(FormattedTextControl(view), height=len(rows) + CONCURRENCY, dont_extend_height=True, always_hide_cursor=True)
    kb = KeyBindings()
    interrupted = False

    @kb.add("c-c")
    def _(event):
        nonlocal interrupted
        interrupted = True
        worker.cancel()
        event.app.exit()

    # full_screen=False keeps us on the normal buffer, so the final frame stays in scrollback;
    # refresh_interval=0 because redraws are driven by the effect below, not by polling
    app = Application(layout=Layout(HSplit([body])), key_bindings=kb, style=STYLE, full_screen=False, color_depth=ColorDepth.TRUE_COLOR, refresh_interval=0)

    async def repaint_on_change():
        # `Application.invalidate` is a no-op until the app is actually running, so the
        # subscription has to be created after run_async has started, not before.
        await app_started.wait()

        @effect
        def redraw():
            view()  # subscribe: any progress row or spinner change schedules a repaint
            app.invalidate()

        try:
            await never
        finally:
            redraw.dispose()

    async def spin():
        while True:
            await sleep(0.05)
            frame.set(next(SPINNER))  # also paces repaints: counters advance faster than any refresh rate

    worker = None

    async def work():
        try:
            return await coro_factory(progress)
        finally:
            app.exit()

    async def ui():
        try:
            return await app.run_async(pre_run=app_started.set)
        finally:
            painter.cancel()

    ticker = create_task(spin())
    painter = create_task(repaint_on_change())
    worker = create_task(work())
    try:
        _, result = await gather(ui(), worker, return_exceptions=True)
    finally:
        ticker.cancel()
        painter.cancel()
        # let both tasks finish unwinding before the loop closes, or a cancelled
        # subprocess transport gets reaped afterwards and prints "Loop ... is closed"
        for task in (ticker, painter, worker):
            with suppress(BaseException):
                await task
    if interrupted or isinstance(result, BaseException):
        raise SyncAbortedError
    return result


def print_summary(store):
    counts = {
        "chats": store.count("chats/*"),
        "messages": store.count("chats/*/messages/*/*.yaml") + store.count("chats/*/threads/*/*.yaml"),
        "users": store.count("users/*"),
        "docs": store.count("docs/*"),
        "minutes": store.count("minutes/*"),
        "meetings": store.count("meetings/*"),
        "bases": store.count("bases/*"),
        "wiki": store.count("wiki/*"),
    }
    width = max(len(k) for k in counts)
    print(f"\n  {store.root}", file=stderr)
    for k, v in counts.items():
        print(f"  {k:<{width}}  {v}", file=stderr)
