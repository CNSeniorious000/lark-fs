"""pnpm-style inline progress: a few lines redrawn in place, no alternate screen."""

from asyncio import CancelledError, Event, Future, create_task, gather, sleep
from contextlib import suppress
from itertools import cycle
from re import compile
from sys import stderr
from xml.sax.saxutils import escape

from prompt_toolkit.application import Application, get_app
from prompt_toolkit.formatted_text import HTML, to_formatted_text
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import HSplit, Layout, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.output import ColorDepth
from prompt_toolkit.styles import Style
from reactivity import derived, effect, signal

from . import cli
from .cli import SyncAbortedError, activity, link_for
from .sync import ALL, Progress, Row

SPINNER = cycle("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
GLYPH = {"pending": "<dim>·</dim>", "running": "", "done": "<ok>✓</ok>", "error": "<err>✗</err>"}
# Brightness encodes "does this still want your attention". A sync screen is mostly
# finished rows, so the brightest hue must belong to what is happening now -- not to the
# ✓ pile, which is what the eye would otherwise be pulled toward. Every pair clears
# WCAG AA (4.5:1) against dark terminal backgrounds; `muted` is the floor at 4.6:1.
STYLE = Style.from_dict(
    {
        "name": "bold #e4e4e7",  # collection names: the page's structure
        "live": "#7dd3fc",  # in flight -- the present, and the brightest thing on screen
        "num": "#fbbf24",  # counts and spinner: the one numeric hue, so a figure is findable
        "ok": "#4ade80",  # the ✓ glyph only; its text goes muted, because it is history
        "err": "#ff8fa3",  # failure reads by hue break, and outranks decoration in weight
        "dom": "#a1a1aa",  # feed column one: a label, deliberately quieter than the subject
        "muted": "#8b8fa3",  # settled subject: legible on demand, invisible when scanning
        "dim": "#8b8fa3",  # header note
    }
)


def _line(row: Row, frame: str, width: int) -> str:
    mark = GLYPH[row["state"]] or f"<num>{frame}</num>"
    done, total = row["done"], row["total"]
    # a fraction needs a denominator worth showing: `0/0` after a ✓ reads as "did nothing"
    # when it means "nothing left to do", and a bare 0 on a pending row looks like a result
    count = "—" if row["state"] == "pending" else f"{done}/{total}" if total else str(done)
    head = f"{mark} <name>{row['name']:<9}</name> <num>{count:<7}</num>"
    # show what was just written; fall back to the phase note when nothing has landed yet
    tail = row["last"] if row["state"] == "running" and row["last"] else row["note"]
    room = max(0, width - max(len(row["name"]), 9) - max(len(count), 7) - 5)
    return head + (f" <dim>{escape(tail[:room])}</dim>" if tail else "")


MARK = {"running": "→", "done": "✓", "error": "✗"}
FEED_MARK = {"running": ("live", "live"), "done": ("ok", "muted"), "error": ("err", "muted")}
RE_TOKEN = compile(r"\b(?:oc_|ou_|obc|tbl|bas)[A-Za-z0-9_]{6,}|\b[A-Za-z0-9]{20,}\b")
MAX_ROWS = 40  # ceiling on total height, so a huge terminal is not filled edge to edge


def _budget(rows: list[str], progress: Progress, height: int) -> dict[str, int]:
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


def _hyperlink(text: str, tone: str) -> list:
    """Fragments for `text`, with any bare token wrapped in an OSC 8 hyperlink.

    A running row often shows nothing but an id; making it clickable turns that from noise
    into the fastest way to open what is being synced. `[ZeroWidthEscape]` is how
    prompt-toolkit passes raw escapes through without counting them toward the width.
    """
    out, at = [], 0
    for m in RE_TOKEN.finditer(text):
        if not (url := link_for(m[0])):
            continue
        out.append((f"class:{tone}", text[at : m.start()]))
        out += [("[ZeroWidthEscape]", f"\x1b]8;;{url}\x1b\\"), (f"class:{tone}", m[0]), ("[ZeroWidthEscape]", "\x1b]8;;\x1b\\")]
        at = m.end()
    out.append((f"class:{tone}", text[at:]))
    return out


def _feed(group: str, limit: int, width: int) -> list[list]:
    """One collection's recent requests, oldest first: finished drift up, in-flight pinned below."""
    entries = activity.rows(group, limit)
    if not entries:
        return []
    col = max(len(domain) for domain, _, _ in entries)
    room = max(8, width - col - 7)  # 4 indent + mark + 2 spaces
    lines = []
    for domain, subject, state in entries:
        glyph, tone = FEED_MARK[state]
        lines.append([("", "    "), (f"class:{glyph}", MARK[state]), ("", " "), ("class:dom", f"{domain:<{col}}"), ("", " "), *_hyperlink(subject[:room], tone)])
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

    cli.feed_enabled = True
    rows = names or ALL
    app_started = Event()
    never = Future()
    progress = Progress()
    for name in rows:
        progress.set(name, state="pending")

    frame = signal(next(SPINNER))

    painted = signal(len(rows))
    peak = len(rows)
    settling = signal(False)  # the last frame keeps only what is on it

    @derived
    def view():
        """Tracks progress.rows and the spinner, so the effect below knows when to redraw."""
        size = get_app().output.get_size()
        width = size.columns
        budget = _budget(rows, progress, min(size.rows - 2, MAX_ROWS))
        lines: list[list] = []
        for n in rows:
            if n not in progress.rows:
                continue
            lines.append(to_formatted_text(HTML(_line(progress.rows[n], frame.get(), width))))
            lines += _feed(n, budget.get(n, 0), width) if budget.get(n) else []
        nonlocal peak
        # holding the peak keeps a shrinking frame from leaving stale rows behind, but that
        # padding would otherwise be frozen into scrollback as a gap the height of the feed
        peak = len(lines) if settling.get() else max(peak, len(lines))
        painted.set(peak)
        lines += [[]] * (peak - len(lines))
        out: list = []
        for i, line in enumerate(lines):
            if i:
                out.append(("", "\n"))
            out += line
        return out

    # height must cover the collection rows plus every concurrent request line
    body = Window(FormattedTextControl(view), height=lambda: painted.get(), dont_extend_height=True, always_hide_cursor=True)
    kb = KeyBindings()
    interrupted = False

    @kb.add("c-c")
    def _stop(event):
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

    async def work():
        try:
            return await coro_factory(progress)
        finally:
            # drop the padding and let one more frame land, or the gap it leaves is what
            # stays in scrollback under the summary. On ctrl-c the sleep re-raises at once,
            # and suppressing that is what keeps `app.exit()` reachable.
            settling.set(True)
            with suppress(CancelledError):
                await sleep(0.05)
            app.exit()

    async def ui():
        try:
            return await app.run_async(pre_run=app_started.set)
        finally:
            painter.cancel()

    worker = create_task(work())
    ticker = create_task(spin())
    painter = create_task(repaint_on_change())
    try:
        _ui, result = await gather(ui(), worker, return_exceptions=True)
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
        "files": store.count("chats/*/files/*/*") - store.count("chats/*/files/*/.oversize"),
    }
    width = max(len(k) for k in counts)
    print(f"  {store.root}", file=stderr)
    for k, v in counts.items():
        print(f"  {k:<{width}}  {v}", file=stderr)
