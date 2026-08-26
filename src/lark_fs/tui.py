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

from .cli import SyncAbortedError, activity
from .sync import ALL, Progress

SPINNER = cycle("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
GLYPH = {"pending": "<dim>·</dim>", "running": "", "done": "<ok>✓</ok>", "error": "<err>✗</err>"}
STYLE = Style.from_dict({
    "ok": "#22c55e",
    "err": "#ef4444",
    "dim": "#6b7280",
    "name": "bold",
    "num": "#eab308",
    "live": "#3b82f6",  # in flight
    "muted": "#6b7280",  # already settled
    "dom": "#a1a1aa",  # feed's first column: readable, but quieter than the collection names
})


def _line(row: dict, frame: str, width: int) -> str:
    mark = GLYPH[row["state"]] or f"<num>{frame}</num>"
    done, total = row["done"], row["total"]
    count = f"{done}/{total}" if total is not None else str(done)
    head = f"{mark} <name>{row['name']:<9}</name> <num>{count:<7}</num>"
    # show what was just written; fall back to the phase note when nothing has landed yet
    tail = row["last"] if row["state"] == "running" and row["last"] else row["note"]
    room = max(0, width - len(row["name"]) - len(count) - 14)
    return head + (f" <dim>{escape(tail[:room])}</dim>" if tail else "")


FEED_MARK = {"running": ("<live>→</live>", "live"), "done": ("<ok>✓</ok>", "muted"), "error": ("<err>✗</err>", "muted")}
MAX_ROWS = 40  # ceiling on total height, so a huge terminal is not filled edge to edge


def _budget(rows: list[str], progress, height: int) -> dict[str, int]:
    """Hand feed lines to whoever is actually working, filling the terminal.

    Every in-flight request is guaranteed a line -- hiding one would misrepresent how much
    is running -- and whatever height remains is shared out so recent history is visible
    too. Idle and finished collections collapse to their own summary line.
    """
    active = [n for n in rows if progress.rows.get(n, {}).get("state") == "running"]
    if not active:
        return {}
    floors = {n: max(1, activity.busy(n)) for n in active}
    spare = max(0, height - len(rows) - sum(floors.values()))
    share, extra = divmod(spare, len(active))
    return {n: floors[n] + share + (1 if i < extra else 0) for i, n in enumerate(active)}


def _feed(group: str, limit: int, width: int) -> list[str]:
    """One collection's recent requests, oldest first: finished drift up, in-flight pinned below."""
    entries = activity.rows(group, limit)
    if not entries:
        return []
    col = max(len(domain) for domain, _, _ in entries)
    room = max(8, width - col - 8)
    lines = []
    for domain, subject, state in entries:
        mark, tone = FEED_MARK[state]
        lines.append(f"    {mark} <dom>{domain:<{col}}</dom> <{tone}>{escape(subject[:room])}</{tone}>")
    return lines


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

    painted = signal(len(rows))
    peak = len(rows)

    @derived
    def view():
        """Tracks progress.rows and the spinner, so the effect below knows when to redraw."""
        width = get_app().output.get_size().columns
        budget = _budget(rows, progress, min(get_app().output.get_size().rows - 2, MAX_ROWS))
        lines = []
        for n in rows:
            if n not in progress.rows:
                continue
            lines.append(_line(progress.rows[n], frame.get(), width))
            lines += _feed(n, budget.get(n, 0), width) if budget.get(n) else []
        nonlocal peak
        peak = max(peak, len(lines))
        painted.set(peak)
        lines += [""] * (peak - len(lines))  # pad, or a shrinking frame leaves stale rows on screen
        return HTML("\n".join(lines))

    # height must cover the collection rows plus every concurrent request line
    body = Window(FormattedTextControl(view), height=lambda: painted.get(), dont_extend_height=True, always_hide_cursor=True)
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
