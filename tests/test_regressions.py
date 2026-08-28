"""Regressions for failures that were silent -- each one shipped and produced plausible output."""

from asyncio import Semaphore, create_task, gather, run, sleep
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from os import utime

import pytest
from yaml import safe_load

from lark_fs import cli
from lark_fs.attachments import Policy, _pending, _settle
from lark_fs.reindex import reindex
from lark_fs.store import Store
from lark_fs.sync import SLICE_HOURS, STAMP, TENANT_TZ, Progress, _edit_signal, _index_media, _note_tenant, _wiki_aliases, _windows, _write_thread, migrate_threads, record_sweep, swept_recently
from lark_fs.yaml import JSON, readable_yaml_dumps


def _schedule(mode: str, seconds: float = 1.5) -> dict[str, int]:
    """Run three collections of very different size for a fixed window; report what each got."""
    work = {"big": 4000, "small": 60, "tiny": 12}

    async def main():
        done = dict.fromkeys(work, 0)
        cli._sem = Semaphore(cli.CONCURRENCY)  # noqa: SLF001

        async def unit(group: str):
            async with cli._sem:  # noqa: SLF001
                await sleep(0.01)
            done[group] += 1

        async def collection(group: str, n: int):
            if mode == "gather":
                await gather(*(unit(group) for _ in range(n)))
            else:
                await cli.spread(lambda _: unit(group), range(n))

        tasks = [create_task(collection(g, n)) for g, n in work.items()]
        await sleep(seconds)
        for t in tasks:
            t.cancel()
        await gather(*tasks, return_exceptions=True)
        return done

    return run(main())


def test_a_large_collection_does_not_starve_the_others():
    """The semaphore queue is FIFO: submitting a whole backlog at once buries everyone behind it."""
    assert _schedule("gather")["small"] == 0, "guard is meaningless if the naive version does not starve"
    assert _schedule("spread")["small"] > 0


def test_bounding_the_queue_costs_no_throughput():
    """The semaphore was always the ceiling, so capping in-flight work only redistributes it."""
    assert sum(_schedule("spread").values()) >= sum(_schedule("gather").values()) * 0.9


def test_urls_survive_entity_decoding():
    """`html.unescape` also decodes entities written without a semicolon, so `&timestamp=`
    in a real URL loses four characters to a multiplication sign."""
    url = "https://x.com/a?b=1&timestamp=2&notify=3&amp;copy=4"
    assert cli.unescape_entities(url) == "https://x.com/a?b=1&timestamp=2&notify=3&copy=4"


NASTY = {
    "leading space": " test3@example.com\nbep!AX93sL!cv@3@",  # a real message; the block indent came from its first line
    "percent": "%%%",  # a real message; % opens a YAML directive
    "u2028": "first\u2028second",  # read as a line break inside a literal block
    "control": "before\x07after\x1b[0m",
    "nul": "a\x00b",
    "crlf": "a\r\nb",
    "tab": "a\tb\nc",
    "backslash and quote": 'say "hi" \\ ok',
    "trailing newlines": "body\n\n\n",
    "only newlines": "\n\n",
    "empty first line": "\nsecond",
    "multiline plus control": " x\ny\x01z",
    "hex literal": "0x1F",  # read back as 31 -- promplate/refined-mcp-servers#24
    "binary literal": "0b1010",
    "octal literal": "0o77",
}


def test_yaml_round_trips_every_character_a_message_can_carry():
    """The mirror parses its own output during reindex, and a message it cannot read back
    is data loss no later run can repair -- the API will not return it again.

    Nothing may be dropped to achieve that: this used to delete control characters and
    U+2028 outright, which parses cleanly and silently returns a different message than
    the one that was sent. A double-quoted scalar carries them as escapes instead."""
    # Nesting is not decoration here: a block scalar's indentation indicator counts from
    # its parent node, so a serializer that writes the absolute column is right at the top
    # level and wrong everywhere else. Messages live two levels down, inside a thread.
    for name, value in NASTY.items():
        shapes: list[JSON] = [{"body": value, "after": "sentinel"}, {"thread": [{"body": value, "after": "sentinel"}]}, {"a": {"b": {"body": value, "after": "sentinel"}}}]
        for shape in shapes:
            loaded = safe_load(readable_yaml_dumps(shape))
            got = loaded.get("body") or (loaded.get("thread") or [{}])[0].get("body") or loaded.get("a", {}).get("b", {}).get("body")
            here = loaded if "body" in loaded else (loaded["thread"][0] if "thread" in loaded else loaded["a"]["b"])
            assert got == value, f"{name} at depth {len(str(shape))}: {got!r} != {value!r}"
            assert here["after"] == "sentinel", f"{name}: the document was restructured"


def test_store_reads_back_what_it_wrote(tmp_path):
    """The mirror parses its own output during reindex; a write it cannot read is data loss."""
    store = Store(tmp_path)
    row = {"id": "om_1", "body": "line\nline still the body", "nested": {"k": ["a", "b"]}}
    store.write_yaml("chats/oc_1/messages/2026-08/om_1.yaml", row)
    assert store.read_yaml("chats/oc_1/messages/2026-08/om_1.yaml")["nested"] == {"k": ["a", "b"]}


def test_a_windowed_sweep_is_labelled_by_its_window():
    """Feed rows for month walks all carry different tokens; the window is what tells them apart."""
    assert cli._label(("minutes", "+search", "--start", "2026-08-01T00:00:00Z", "--page-size", "30")) == ("minutes", "search 2026-08-01")  # noqa: SLF001


def test_windows_tile_the_range_without_gaps_or_overlap():
    """A busy chat is walked as parallel windows; a seam that drifts loses or doubles history."""
    windows = _windows("2026-04-06 00:00", SLICE_HOURS)
    assert windows[0][0] == "2026-04-06 00:00"
    assert all(end == nxt for (_, end), (nxt, _) in pairwise(windows))


def test_windows_reach_past_now():
    """Stopping at the last whole window would leave today's messages unreachable."""
    assert _windows("2026-04-06 00:00", SLICE_HOURS)[-1][1] > datetime.now(TENANT_TZ).strftime(STAMP)


def test_attachment_policy_admits_only_configured_kinds(tmp_path):
    """Every admitted attachment is one request against a monthly quota; a wrong kind is spend."""
    store = Store(tmp_path)
    rows = [
        {"key": "file_1", "name": "notes.md", "chat_id": "oc_1", "message_id": "om_1"},
        {"key": "file_2", "name": "clip.mp4", "chat_id": "oc_1", "message_id": "om_2"},
        {"key": "img_3", "name": "", "chat_id": "oc_1", "message_id": "om_3"},
        {"key": "file_4", "name": "SHOUTED.MD", "chat_id": "oc_1", "message_id": "om_4"},
    ]
    assert [r["key"] for r in _pending(store, Policy({"text"}, 1), rows)] == ["file_1", "file_4"]
    assert [r["key"] for r in _pending(store, Policy({"text", "video", "image"}, 1), rows)] == ["file_1", "file_2", "img_3", "file_4"]


def test_an_oversize_marker_stops_the_retry(tmp_path):
    """The marker is the only record that a file was too big -- the file itself is deleted."""
    store = Store(tmp_path)
    row = {"key": "file_1", "name": "huge.md", "chat_id": "oc_1", "message_id": "om_1"}
    store.write(f"chats/oc_1/files/{row['key']}/.oversize", str(30 * 1024 * 1024))
    assert _pending(store, Policy({"text"}, 1), [row]) == []
    # a marker from before sizes were recorded says nothing about the current cap, so it
    # is worth one more fetch -- which is what fills the size in
    store.write(f"chats/oc_1/files/{row['key']}/.oversize", "")
    assert _pending(store, Policy({"text"}, 1), [row]) == [row]


def test_an_extension_escape_hatch_beats_the_kind_lists(tmp_path):
    """The built-in lists will always miss a tenant's own conventions."""
    store = Store(tmp_path)
    rows = [{"key": "file_1", "name": "dump.prompt", "chat_id": "oc_1", "message_id": "om_1"}]
    assert _pending(store, Policy({"text"}, 1), rows) == []
    assert _pending(store, Policy({"text"}, 1, [".PROMPT"]), rows) == rows


def test_the_tenant_host_is_learned_never_hardcoded():
    """Every install has a different one and no API reports it; a baked-in host ships
    one tenant's domain to everybody else."""
    before = cli.TENANT
    try:
        cli.TENANT = ""
        _note_tenant("url: 'https://acme.feishu.cn/wiki/AbC123'")
        assert cli.TENANT == "https://acme.feishu.cn"
        _note_tenant("https://other.larksuite.com/docx/X")
        assert cli.TENANT == "https://acme.feishu.cn", "first one wins; a later doc must not move it"
    finally:
        cli.TENANT = before


def test_links_degrade_instead_of_pointing_at_the_wrong_tenant():
    """Before the host is known, a doc link would be wrong -- so there is none. Chat and
    person links go through applink, which is tenant-independent, and still work."""
    before = cli.TENANT
    try:
        cli.TENANT = ""
        assert cli.link_for("AbC123wiki") == ""
        assert cli.link_for("oc_1a2b3c4d5e6f").startswith("https://applink.feishu.cn/")
    finally:
        cli.TENANT = before


def test_a_wiki_node_token_resolves_to_the_document_it_points_at(tmp_path):
    """Drive answers 1069307 for a node token, and mirroring under both names stores the
    same document twice -- the node list is the only thing that can translate them."""
    store = Store(tmp_path)
    store.write_yaml(
        "wiki/7383/nodes.yaml",
        [
            {"node_token": "Nod1", "obj_token": "Obj1", "obj_type": "docx", "title": "a"},
            {"node_token": "Nod2", "obj_type": "docx", "title": "no obj_token"},
        ],
    )
    assert _wiki_aliases(store) == {"Nod1": "Obj1"}


def test_raising_the_size_cap_brings_back_what_it_rejected(tmp_path):
    """The marker has to carry the size that disqualified the file, or the first cap that
    ever rejected it becomes permanent and no config change can undo it."""
    store = Store(tmp_path)
    row = {"key": "file_1", "name": "big.jsonl", "chat_id": "oc_1", "message_id": "om_1"}
    store.write(f"chats/oc_1/files/{row['key']}/.oversize", str(30 * 1024 * 1024))
    assert _pending(store, Policy({"text"}, 10 * 1024 * 1024), [row]) == [], "still too big at 10MB"
    assert _pending(store, Policy({"text"}, 50 * 1024 * 1024), [row]) == [row], "fits at 50MB, fetch it again"


def test_the_marker_records_the_size_that_disqualified_it(tmp_path):
    """Writing a bare marker is what made the first rejecting cap permanent."""
    dest = tmp_path / "file_1"
    dest.mkdir()
    big = dest / "big.jsonl"
    big.write_text("x")
    assert _settle(dest, {"saved_path": str(big), "size_bytes": 30_000_000}, 10_000_000) is False
    assert not big.exists(), "an oversize file is not kept"
    assert (dest / ".oversize").read_text() == "30000000"


def _graph_probe(monkeypatch, sweep: float = 0.08) -> list[str]:
    """Replace every collection with a recorder, so what is measured is the dependency
    graph rather than any API. The sweep is the only slow one, as it is in a real run."""
    from lark_fs import sync as sync_module

    order: list[str] = []

    def stub(name: str, delay: float = 0.0, ret=None):
        async def record(*_a, **_k):
            order.append(f"{name}:start")
            if delay:
                await sleep(delay)
            order.append(f"{name}:done")
            return ret

        return record

    monkeypatch.setattr(sync_module, "_learn_tenant", lambda _store: None)
    monkeypatch.setattr(sync_module, "_list_chats", stub("roster", 0.0, {"oc_1"}))
    monkeypatch.setattr(sync_module, "sync_messages", stub("messages", sweep, {"oc_1"}))
    monkeypatch.setattr(sync_module, "sync_chat_meta", stub("chats"))
    for name in ("sync_wiki", "sync_docs", "sync_minutes", "sync_meetings", "sync_bases"):
        monkeypatch.setattr(sync_module, name, stub(name.removeprefix("sync_")))
    monkeypatch.setattr(sync_module, "sync_attachments", stub("files"))
    return order


def test_a_pass_nobody_awaits_is_not_abandoned(tmp_path, monkeypatch):
    """`gather` returns as soon as the listed tasks finish, so a producer left off that
    list keeps running into a process that is already printing its summary -- the sweep
    stops mid-flight and the run still reports success."""
    from lark_fs.sync import sync_all

    order = _graph_probe(monkeypatch)
    run(sync_all(tmp_path, Progress(), ["messages"]))
    assert "messages:done" in order, "the sweep was abandoned when gather returned"


def test_the_roster_pass_does_not_wait_out_the_message_sweep(tmp_path, monkeypatch):
    """Both passes want the same one-request roster. Taking it from the sweep's return
    value instead made a pass that is usually a no-op sit through every message in every
    chat first -- and put its cheap work after the rate limit that interrupts the sweep."""
    from lark_fs.sync import sync_all

    order = _graph_probe(monkeypatch)
    run(sync_all(tmp_path, Progress(), ["messages", "chats"]))
    assert order.index("chats:done") < order.index("messages:done"), f"roster pass waited for the sweep: {order}"


def test_an_unnamed_image_is_named_by_its_bytes(tmp_path):
    """Rich-text images carry no filename, so the mirror stores them under the bare key --
    `fd -e jpg` misses them and nothing opens them. Lark reports no content type either,
    and the key says `img_` without saying which kind, so the bytes are the only source."""
    dest = tmp_path / "img_v3_abc"
    dest.mkdir()
    saved = dest / "img_v3_abc"
    saved.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 20)  # the JPEG one measured on a real post image
    assert _settle(dest, {"saved_path": str(saved), "size_bytes": 24}, 1_000) is True
    assert (dest / "img_v3_abc.jpg").is_file(), sorted(p.name for p in dest.iterdir())


def test_a_named_attachment_keeps_the_name_lark_gave_it(tmp_path):
    """Sniffing must not rewrite a filename the sender chose; `.md` stays `.md`."""
    dest = tmp_path / "file_v3_abc"
    dest.mkdir()
    saved = dest / "notes.md"
    saved.write_bytes(b"# hi\n")
    _settle(dest, {"saved_path": str(saved), "size_bytes": 5}, 1_000)
    assert (dest / "notes.md").is_file()


def _nested_thread() -> dict:
    """A thread as Lark hands it over: the root, with every reply inlined."""
    return {
        "message_id": "om_root",
        "chat_id": "oc_1",
        "thread_id": "omt_1",
        "create_time": "2026-08-24 23:04",
        "content": "Effect why slower",
        "sender": {"id": "ou_root", "id_type": "open_id", "name": "a"},
        "thread_replies": [
            {"message_id": "om_r1", "chat_id": "oc_1", "create_time": "2026-08-24 23:47", "content": "rust?", "sender": {"id": "ou_r1", "id_type": "open_id", "name": "b"}},
            {"message_id": "om_r2", "chat_id": "oc_1", "create_time": "2026-08-24 23:48", "content": "![Image](img_v3_deep)", "sender": {"id": "ou_r1", "id_type": "open_id", "name": "b"}},
        ],
    }


def test_every_thread_reply_gets_its_own_file(tmp_path):
    """Stored as one nested blob, a reply has no file named by its id -- and `fd <message_id>`
    is the mirror's primary query interface. Measured before this: 49308 such replies."""
    store = Store(tmp_path)
    _write_thread(store, "oc_1", "omt_1", _nested_thread())
    for mid in ("om_root", "om_r1", "om_r2"):
        assert store.exists(f"chats/oc_1/threads/omt_1/{mid}.yaml"), mid
    assert "thread_replies" not in store.read_yaml("chats/oc_1/threads/omt_1/om_root.yaml"), "the root must not keep a second copy"
    assert store.read_yaml("chats/oc_1/threads/omt_1/meta.yaml")["replies"] == 2


def test_the_root_keeps_its_own_id_as_a_filename(tmp_path):
    """A thread root is never also written under messages/ (measured: 0 of 400), so naming
    its file meta.yaml would put 16717 real messages out of reach of `fd`."""
    store = Store(tmp_path)
    _write_thread(store, "oc_1", "omt_1", _nested_thread())
    assert store.read_yaml("chats/oc_1/threads/omt_1/om_root.yaml")["message_id"] == "om_root"


def test_migration_reaches_replies_the_projections_never_saw(tmp_path):
    """The sweep only revisits a chat from its cursor forward, so replies already on disk
    would stay invisible forever. Migration runs first and the same pass then indexes them:
    3254 attachment keys and 912 people were missing from a real mirror."""
    store = Store(tmp_path)
    store.write_yaml("chats/oc_1/threads/omt_1/om_root.yaml", _nested_thread())  # the old shape
    counts = reindex(tmp_path)
    assert counts["threads_split"] == 1
    assert store.exists("users/ou_r1/meta.yaml"), "a reply's sender was never in users/"
    assert "img_v3_deep" in {r["key"] for r in store.glob_rows("chats/*/media.yaml")}, "a reply's attachment was never indexed"


def test_migration_does_not_reparse_an_already_split_store(tmp_path):
    """It runs at the head of every sync, so a store that is already split must cost a stat
    per thread rather than a parse of every message in it."""
    store = Store(tmp_path)
    _write_thread(store, "oc_1", "omt_1", _nested_thread())
    assert migrate_threads(store) == 0


def test_a_thread_with_no_replies_still_gets_its_marker(tmp_path):
    """meta.yaml is what the migration scan short-circuits on, and it runs at the head of
    every sync. A thread that never earns one is re-parsed forever: the 4677 unreplied
    threads in a real mirror were 3.6s of the 3.7s an already-migrated store paid."""
    store = Store(tmp_path)
    store.write_yaml("chats/oc_1/threads/omt_2/om_lonely.yaml", {"message_id": "om_lonely", "chat_id": "oc_1", "thread_id": "omt_2", "content": "no replies"})
    assert migrate_threads(store) == 0, "nothing was split -- it had no replies to split"
    assert store.exists("chats/oc_1/threads/omt_2/meta.yaml"), "but it must still be marked, or it is re-read every sync"
    assert migrate_threads(store) == 0


def test_a_truncated_thread_is_remembered_for_repair(tmp_path):
    """A chat listing inlines at most 50 replies and sets `thread_has_more`. The sweep
    moves its cursor past that chat and never returns, so a thread that is not written
    down here keeps its missing replies forever -- 57 threads in a real mirror, one of
    them short by 14."""
    store = Store(tmp_path)
    msg = {"message_id": "om_root", "chat_id": "oc_1", "thread_id": "omt_1", "thread_has_more": True, "thread_replies": [{"message_id": "om_r1", "chat_id": "oc_1"}]}
    _write_thread(store, "oc_1", "omt_1", msg)
    assert store.cursors["threads_incomplete"] == {"omt_1": "oc_1"}
    assert store.read_yaml("chats/oc_1/threads/omt_1/meta.yaml")["has_more"] is True


def test_a_whole_thread_is_not_put_on_the_repair_list(tmp_path):
    """Every thread would otherwise be re-fetched through the slower per-thread endpoint."""
    store = Store(tmp_path)
    _write_thread(store, "oc_1", "omt_1", _nested_thread())
    assert not store.cursors.get("threads_incomplete")
    assert store.read_yaml("chats/oc_1/threads/omt_1/meta.yaml")["has_more"] is False


def test_a_message_body_is_not_always_a_string():
    """One real message is a 48-digit number, which the API hands over as a JSON number.
    It round-trips as an int, and every reader that treats a body as text crashed on it --
    taking down a reindex over the whole store, thousands of messages later."""
    msg = {"message_id": "om_1", "chat_id": "oc_1", "content": 147399225000731836122715425445934994596181150067}
    assert _index_media(msg) == []
    assert _edit_signal(msg) == ("", "147399225000731836122715425445934994596181150067", False)


def test_every_codepoint_round_trips():
    """A hand-picked set of nasty inputs is a guess about which characters are dangerous,
    and the guess has been wrong three times: it covered C0 and missed C1, so a real message
    carrying U+009A stayed unreadable through two rounds of repair; then it stopped at
    U+2200 and missed the surrogates and U+FFFE/U+FFFF; then every shape it tried was a
    mapping, so the BOM went unnoticed. All three were misses in the *range or the shapes*,
    never in the reasoning about a character once it was actually tried.

    The shapes matter as much as the range. A character is only dangerous in some
    positions -- ` a\nb` ends its block early, `\n a\n b` loses the spaces in silence,
    and a BOM is content everywhere except offset 0 of the stream, which only a top-level
    bare string reaches. So each codepoint is tried nested and bare."""
    for cp in range(0x11000):  # past the BMP, so the surrogates and noncharacters are in
        ch = chr(cp)
        for value in (f"x{ch}y", f"a\n{ch}b", f"\n {ch}c", f"\n  \n {ch}", f"{ch}\n a\n  b"):
            loaded = safe_load(readable_yaml_dumps({"body": value, "after": "sentinel"}))
            assert loaded["body"] == value, f"U+{cp:04X} in {value!r} did not survive: {loaded['body']!r}"
            assert loaded["after"] == "sentinel", f"U+{cp:04X} in {value!r} restructured the document"
        # bare, with nothing in front of the value: the only way anything reaches offset 0
        for value in (f"{ch}top", f"to{ch}p", f"{ch}first\nsecond"):
            assert safe_load(readable_yaml_dumps(value)) == value, f"U+{cp:04X} in bare {value!r} did not survive"


def test_a_profile_row_is_filtered_before_it_lands(tmp_path, monkeypatch):
    """`+search-user` answers with more than a profile: `chat_recency_hint` is relative to
    now ("Contacted today") and `match_segments` echoes the query, so merging the row
    wholesale rewrites meta.yaml on every sync and buries the roster's own `name`."""
    from lark_fs import sync as sync_module

    async def fake_run(*argv, **_):
        if argv[1] == "+get-user":
            return {"user": {"tenant_key": "T"}}
        ids = argv[argv.index("--user-ids") + 1].split(",")
        return {"users": [{"open_id": i, "name": "", "localized_name": "Mia(张亚)", "chat_recency_hint": "Contacted today", "match_segments": [], "has_chatted": True} for i in ids]}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("users/ou_1/meta.yaml", {"open_id": "ou_1", "member_id": "ou_1", "name": "张亚", "tenant_key": "T"})

    run(sync_module.sync_profiles(store, Progress()))

    got = store.read_yaml("users/ou_1/meta.yaml")
    assert got["localized_name"] == "Mia(张亚)", "the alias that disambiguates same-named people was not taken"
    assert got["name"] == "张亚" and got["member_id"] == "ou_1", f"the roster's own fields were overwritten: {got}"
    assert not {"chat_recency_hint", "match_segments", "has_chatted"} & got.keys(), f"volatile fields landed on disk: {got}"


def test_a_discovery_pass_coasts_but_an_explicit_request_never_does(tmp_path):
    """The wiki walk and the doc search probes were 690 of one sync's 1247 requests, and
    neither can be made incremental -- a wiki node carries no update time and a search is
    a ranked slice. Frequency is the only lever, so a plain sync lets them coast."""
    store = Store(tmp_path)
    assert swept_recently(store, "wiki", 24) is False, "never swept, so it is due"
    record_sweep(store, "wiki")
    assert swept_recently(store, "wiki", 24) is True, "just swept, so it coasts"
    assert swept_recently(store, "docs", 24) is False, "each pass keeps its own clock"


def test_a_sweep_comes_due_again(tmp_path):
    """A clock that never expires is not a schedule, it is a one-shot."""
    store = Store(tmp_path)
    record_sweep(store, "wiki")
    store.cursors["swept"]["wiki"] = (datetime.now(UTC) - timedelta(hours=25)).isoformat(timespec="seconds")
    assert swept_recently(store, "wiki", 24) is False


def test_a_permanent_failure_is_remembered_not_retried_forever():
    """A sheet has no markdown body and never will, but the fetch failed the same way a
    rate limit does and was treated the same: 146 of them were re-requested on every sync,
    31% of the run. `is_missing` already drew this line for a different code."""
    unsupported = cli.LarkError(["docs", "+fetch"], {"error": {"code": 3380002, "message": "Unsupported document type 'bitable'. Only docx is supported."}})
    assert unsupported.is_unsupported_type
    assert not unsupported.is_rate_limited, "marking a rate limit permanent would discard the document for good"
    throttled = cli.LarkError(["docs", "+fetch"], {"error": {"code": 99991400}})
    assert not throttled.is_unsupported_type


def test_a_sheet_is_asked_for_as_a_sheet(tmp_path, monkeypatch):
    """`--type` was pinned to `docx`, and Drive answers 1069307 for a sheet asked for as
    one -- the same code that means "this token does not exist", which we then remember as
    `.nocomments`. 220 sheets and bitables lost their comments to a mistake of our own.

    The `+fetch` failure is the second half: only docx exports markdown, so a sheet's body
    fetch fails permanently, and returning there skips the comments pass for good.
    """
    from lark_fs import sync as sync_module

    asked: list[tuple[str, str]] = []

    async def fake_run(*argv, **_):
        if argv[1] == "+fetch":
            raise cli.LarkError(list(argv), {"error": {"code": 3380002, "message": "Unsupported document type 'sheet'."}})
        asked.append((argv[argv.index("--token") + 1], argv[argv.index("--type") + 1]))
        return {"items": [{"comment_id": "c1"}]}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("wiki/s1/nodes.yaml", [{"node_token": "n1", "obj_token": "tok1", "obj_type": "sheet", "title": "roster"}])

    run(sync_module.sync_docs(store, Progress(), search=False))

    assert asked == [("tok1", "sheet")], f"a sheet was not asked for as a sheet: {asked}"
    assert store.exists("docs/tok1/comments.yaml"), "the body fetch failing took the comments with it"
    assert store.exists("docs/tok1/.nobody"), "a document that can never have a body was left due"


def test_a_failed_sweep_does_not_claim_its_window(tmp_path, monkeypatch):
    """The stamp used to go down on the way in, so a pass that died on its first request
    coasted the whole window having fetched nothing -- one rate-limited `+space-list` cost
    a day of wiki instead of a run of it. Failure has to come due sooner than success."""
    from lark_fs import sync as sync_module

    async def fake_run(*argv, **_):
        raise cli.LarkError(list(argv), {"error": {"code": 99991400, "message": "rate limit"}})

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)

    run(sync_module.sync_wiki(store, Progress()))
    assert swept_recently(store, "wiki", 24) is False, "a wiki walk that fetched nothing claimed the day anyway"

    run(sync_module.sync_docs(store, Progress(), search=True))
    assert swept_recently(store, "docs", 6) is False, "a search that answered nothing claimed the window anyway"


def test_a_sweep_cut_short_does_not_claim_its_window(tmp_path, monkeypatch):
    """A query refused mid-pagination loses every page after it, and `except LarkError`
    swallows that -- so the pass looks like it succeeded. Measured against the real
    endpoint: run sequentially it answers 14 of 14 for 4762 hits, but spread three-wide
    nine queries were cut short by 99991400 and 2840 of those hits never arrived.

    Coasting six hours on a corpus missing 60% of itself is worse than paying for the pass
    again, and the limit that caused it clears in seconds."""
    from lark_fs import sync as sync_module

    async def flaky_run(*argv, **_):
        if argv[1] != "+search":
            raise cli.LarkError(list(argv), {"error": {"code": 1069307}})
        if argv[argv.index("--query") + 1] != "":
            raise cli.LarkError(list(argv), {"error": {"code": 99991400, "message": "rate limit"}})
        return {"results": [{"entity_type": "DOC", "title_highlighted": "found", "result_meta": {"token": "tok1", "url": ""}}]}

    monkeypatch.setattr(cli, "run", flaky_run)
    store = Store(tmp_path)

    run(sync_module.sync_docs(store, Progress()))

    assert store.exists("docs/tok1/meta.yaml"), "the one query that answered was not written"
    assert swept_recently(store, "docs", 6) is False, "a sweep that lost 13 of 14 queries claimed its six hours"


def test_every_document_kind_is_named_the_way_drive_names_it():
    """A wiki node calls the type `obj_type`; a search hit calls it `doc_types`, upper case.
    Only the first was read, so the 203 non-docx documents that search alone found were
    still asked for as `docx` -- and Drive answers 1069307 for those, which is remembered
    as "has no comments" forever. Measured with a real token of each kind: asked as itself
    every one answers, asked as `docx` every one answers 1069307.

    `mindnote` is the exception the endpoint does not name at all, so it falls back."""
    from lark_fs.sync import _doc_type

    assert _doc_type({"obj_type": "sheet"}) == "sheet", "the wiki node's own field was ignored"
    assert _doc_type({"doc_types": "BITABLE"}) == "bitable", "a search hit's type was ignored"
    assert _doc_type({"file_type": "SLIDES"}) == "slides"
    assert _doc_type({"doc_types": "MINDNOTE"}) == "docx", "a type the endpoint cannot name must fall back, not be sent"
    assert _doc_type({}) == "docx", "no type at all is the docx case this started as"


def test_an_unresolved_wiki_hit_is_addressed_as_a_wiki_node(tmp_path, monkeypatch):
    """A WIKI search hit carries a node token, and `_wiki_aliases` turns it into the
    document's own -- but only for spaces whose node list is on disk. For the rest the node
    token is all there is, and Drive answers 1069307 for it as the docx it wraps while
    answering it as `wiki`. The reverse holds for a resolved token: `wiki` gets 131005.

    Measured: 860 resolved hits answer as their own type, 19 unresolved ones only as
    `wiki`, and neither addressing works for the other group -- so this cannot be a blanket
    rule, and it is not something `_doc_type` can tell from the record alone."""
    from lark_fs import sync as sync_module

    asked: list[tuple[str, str]] = []

    async def fake_run(*_argv, **_):
        return {"document": {"content": "body"}}

    async def fake_paginate(*argv, **_k):
        if argv[1] == "+search":
            for hit in hits:
                yield hit
            return
        asked.append((argv[argv.index("--token") + 1], argv[argv.index("--type") + 1]))

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("wiki/s1/nodes.yaml", [{"node_token": "node_known", "obj_token": "obj_1", "obj_type": "docx", "title": "resolved"}])

    hits = [
        {"entity_type": "WIKI", "title_highlighted": "resolved", "result_meta": {"token": "node_known", "doc_types": "DOCX", "url": ""}},
        {"entity_type": "WIKI", "title_highlighted": "stranded", "result_meta": {"token": "node_orphan", "doc_types": "DOCX", "url": ""}},
    ]
    monkeypatch.setattr(cli, "paginate", fake_paginate)

    run(sync_module.sync_docs(store, Progress(), queries=["q"]))

    assert ("obj_1", "docx") in asked, f"a resolved hit must be asked for as its own type: {asked}"
    assert ("node_orphan", "wiki") in asked, f"an unresolved node token is only known to Drive as a wiki: {asked}"


async def _aiter(items):
    for item in items:
        yield item


def test_a_quoted_value_is_parsed_not_stripped(tmp_path):
    """`_rows` is a hand-rolled reader -- a real YAML parser on a 40k-row media index is
    seconds instead of milliseconds -- but it only stripped the quote characters. A
    single-quoted scalar doubles its own quote and a double-quoted one carries escapes, so
    stripping returns a different string than was written.

    The mirror already holds one: an attachment named `... C'est l'application ....mp4`,
    which `readable_yaml_dumps` writes double-quoted and which came back with the quotes
    themselves inside the filename -- and that name is what the file is saved as."""
    store = Store(tmp_path)
    names = [
        "Si toi aussi tu veut télécharger C'est l'application ... #fyp.mp4",  # real, chats/oc_6de932.../media.yaml
        'say "hi".txt',
        "back\\slash.txt",
        "plain.txt",
        "0x1F",  # quoted for a different reason: it would read back as the integer 31
    ]
    rows = [{"key": f"file_{i}", "name": n} for i, n in enumerate(names)]
    store.write_yaml("chats/oc_1/media.yaml", rows)

    assert [r["name"] for r in store.read_yaml_rows("chats/oc_1/media.yaml")] == names


def test_a_table_that_paged_short_is_not_frozen_as_whole(tmp_path, monkeypatch):
    """`records.yaml` is written once and skipped forever after -- so a rate limit on the
    third page used to persist the first 200 rows as if they were the table. Nothing on
    disk says otherwise and no later run looks again."""
    from lark_fs import sync as sync_module

    async def fake_run(*argv, **_):
        if argv[1] == "+table-list":
            return {"tables": [{"id": "tbl_1"}]}
        if argv[argv.index("--offset") + 1] == "0":
            return {"fields": ["a"], "data": [["1"]], "record_id_list": ["rec_1"], "has_more": True}
        raise cli.LarkError(list(argv), {"error": {"code": 99991400, "message": "rate limit"}})

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "paginate", lambda *_a, **_k: _aiter([{"result_meta": {"token": "bas_1", "doc_types": "BITABLE"}}]))
    store = Store(tmp_path)

    run(sync_module.sync_bases(store, Progress()))

    assert store.exists("bases/bas_1/tables/tbl_1/meta.yaml"), "the test never reached the record loop"
    assert not store.exists("bases/bas_1/tables/tbl_1/records.yaml"), "a partial table was frozen in place as the whole one"


def test_a_failed_repair_request_does_not_clear_the_queue(tmp_path, monkeypatch):
    """`messages_unreadable` is the only record that a file on disk cannot be parsed --
    `reindex` builds it because scanning every message during a sync is far too slow. The
    repair returned `[]` both when the server had dropped a message and when the request
    itself failed, and the caller cleared the whole chunk either way. A rate limit there
    dropped 50 still-broken files off the list, past a cursor that had already moved on."""
    from lark_fs import sync as sync_module

    async def fake_run(*argv, **_):
        if argv[1] == "+chat-list":
            return {"chats": []}
        raise cli.LarkError(list(argv), {"error": {"code": 99991400, "message": "rate limit"}})

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "paginate", lambda *_a, **_k: _aiter([]))
    store = Store(tmp_path)
    store.cursors["messages_unreadable"] = {"om_1": "chats/oc_1/messages/2026-08/om_1.yaml"}

    run(sync_module.sync_messages(store, Progress(), chat_ids=set()))

    assert store.cursors["messages_unreadable"] == {"om_1": "chats/oc_1/messages/2026-08/om_1.yaml"}, "a failed request emptied the repair queue"


def test_the_media_index_is_durable_before_the_cursor_is(tmp_path, monkeypatch):
    """Cursors are saved per chat; media rows lived in memory until every chat was done.
    Interrupt in between -- ctrl-c, an exhausted quota, another collection raising -- and
    the cursor is past messages whose attachments were never indexed. Nothing walks those
    messages again, and only a full `reindex` finds them.

    The interrupt lands the instant the cursor becomes durable, which is the only moment
    that tells the two orderings apart."""
    from lark_fs import sync as sync_module

    msg = {"message_id": "om_1", "create_time": "2026-08-01 10:00", "chat_id": "oc_1", "msg_type": "file", "content": '<file key="file_v3_001_abc" name="a.txt">'}
    monkeypatch.setattr(cli, "paginate", lambda *_a, **_k: _aiter([msg]))

    store = Store(tmp_path)
    real_save, saved = store.save_cursors, []

    def save_then_die():
        real_save()
        saved.append(1)
        raise KeyboardInterrupt

    monkeypatch.setattr(store, "save_cursors", save_then_die)

    with suppress(KeyboardInterrupt):
        run(sync_module.sync_messages(store, Progress(), chat_ids={"oc_1"}))

    assert saved, "the cursor never became durable, so the test proves nothing"
    assert store.read_yaml_rows("chats/oc_1/media.yaml"), "the cursor outlived the media index it was supposed to follow"


def test_raising_the_cap_settles_the_file_it_brings_back(tmp_path):
    """The marker carries the size that disqualified a file so raising `max_mb` brings it
    back -- but the download that brings it back left the marker in place, and `_settled`
    reads a marker as "not settled at this cap". The bytes are on disk and fetched again on
    every run, forever."""
    store = Store(tmp_path)
    row = {"key": "file_1", "name": "big.md", "chat_id": "oc_1", "message_id": "om_1"}
    dest = store.root / "chats/oc_1/files/file_1"
    dest.mkdir(parents=True)
    (dest / ".oversize").write_text(str(30 * 1024 * 1024))
    (dest / "big.md").write_text("x")

    assert _settle(dest, {"saved_path": str(dest / "big.md"), "size_bytes": 30 * 1024 * 1024}, 40 * 1024 * 1024)
    assert _pending(store, Policy({"text"}, 40 * 1024 * 1024), [row]) == [], "the file is on disk and still queued for download"


def test_an_empty_body_is_not_an_empty_body_forever(tmp_path):
    """`.nobody` meant two different things: "this is a sheet, and only docx exports
    markdown" -- permanent -- and "this docx had nothing in it when we looked", which stops
    being true the moment somebody types in it. Read alike, the second froze the document
    out of every later run."""
    from lark_fs.sync import _doc_is_stale

    store = Store(tmp_path)
    later = datetime.now(UTC).timestamp() + 3600

    store.write("docs/sheet_1/.nobody", "unsupported")
    assert _doc_is_stale(store, "sheet_1", {"update_time": later}) is False, "a sheet was re-fetched for a body it can never have"

    store.write("docs/doc_1/.nobody", "")
    assert _doc_is_stale(store, "doc_1", {"update_time": later}) is True, "a docx edited after we saw it empty stayed empty on disk"
    assert _doc_is_stale(store, "doc_1", {"update_time": 1}) is False, "an untouched empty document was re-fetched anyway"


def test_a_window_at_its_ceiling_is_split_not_accepted():
    """`+search` caps the *total* a query returns, not a page, so a window that reaches the
    cap is the first N of an unknown number -- the rest are unreachable through that query
    however its pages are walked. A month was assumed narrow enough. Measured on disk: 159
    meetings in 2026-07 and 152 in 2026-06, against a ceiling of 150."""
    from lark_fs.sync import _sweep_window

    # one busy day inside the range; every window overlapping it comes back at the ceiling
    busy = datetime(2026, 7, 14, tzinfo=UTC)
    asked: list[tuple[str, str]] = []

    async def take(lo, hi):
        asked.append((lo.strftime("%Y-%m-%d"), hi.strftime("%Y-%m-%d")))
        return 150 if lo <= busy < hi else 3

    run(_sweep_window(datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 8, 1, tzinfo=UTC), 150, take))

    assert asked[0] == ("2026-07-01", "2026-08-01"), "the whole month should be tried first"
    narrowest = min(asked, key=lambda w: datetime.fromisoformat(w[1]) - datetime.fromisoformat(w[0]))
    assert (datetime.fromisoformat(narrowest[1]) - datetime.fromisoformat(narrowest[0])).days == 1, f"never narrowed to a day: {asked}"
    assert all(a[0] != a[1] for a in asked), f"an empty window was queried: {asked}"


def test_splitting_stops_where_the_api_stops():
    """A single day that still fills the cap cannot be split finer -- `--start`/`--end` take
    a date. Recursing past that is an infinite loop, not a smaller window."""
    from lark_fs.sync import _sweep_window

    asked = []

    async def always_full(lo, hi):
        asked.append((lo, hi))
        return 150

    run(_sweep_window(datetime(2026, 7, 1, tzinfo=UTC), datetime(2026, 7, 2, tzinfo=UTC), 150, always_full))
    assert len(asked) == 1, f"a one-day window was split further: {asked}"


def test_the_recheck_walks_the_window_instead_of_replaying_it(tmp_path, monkeypatch):
    """The daemon runs this tier every 30 minutes and it replayed every message in the
    window each time -- 2431 sequential requests on the real store, which takes longer than
    the 30 minutes until it is due again, so the daemon never left it. Roughly 115k requests
    a day against a monthly tenant quota.

    Half the budget sweeps the rest of the window across runs, resumed from the last id
    rather than an offset, because the list shifts as messages arrive and age out."""
    from lark_fs import sync as sync_module

    asked: list[str] = []

    async def fake_run(*argv, **_):
        chunk = argv[argv.index("--message-ids") + 1].split(",")
        asked.extend(chunk)
        return {"messages": [{"message_id": m, "update_time": "1", "body": {}} for m in chunk]}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    month = datetime.now(UTC).strftime("%Y-%m")
    for i in range(20):
        store.write_yaml(f"chats/oc_1/messages/{month}/om_{i:03}.yaml", {"message_id": f"om_{i:03}"})

    run(sync_module.recheck_messages(store, Progress(), batch=2, budget=2))
    assert len(asked) == 4, f"the budget was not honoured: {len(asked)}"
    first_cold = store.cursors["recheck_after"]

    asked.clear()
    run(sync_module.recheck_messages(store, Progress(), batch=2, budget=2))
    assert store.cursors["recheck_after"] > first_cold, "the sweeping half did not advance past where it stopped"


def test_the_newest_files_are_checked_every_run(tmp_path, monkeypatch):
    """An edit lands within minutes of the message, and message ids do not encode that:
    measured over the real window their sort order runs *against* create_time (Kendall tau
    -0.73), so a cursor alone reaches a new message at a random point in its cycle -- 15
    hours in on average, against the 30 minutes this used to take."""
    from lark_fs import sync as sync_module

    asked: list[str] = []

    async def unchanged(*argv, **_):
        # answer exactly what is on disk, so nothing is rewritten: this test is about which
        # ids are chosen, and a rewrite would reset the mtime that choice is made on
        chunk = argv[argv.index("--message-ids") + 1].split(",")
        asked.extend(chunk)
        return {"messages": [{"message_id": m} for m in chunk]}

    monkeypatch.setattr(cli, "run", unchanged)
    store = Store(tmp_path)
    month = datetime.now(UTC).strftime("%Y-%m")
    stale = datetime.now(UTC).timestamp() - 86400
    for i in range(20):
        rel = f"chats/oc_1/messages/{month}/om_{i:03}.yaml"
        store.write_yaml(rel, {"message_id": f"om_{i:03}"})
        utime(store.root / rel, (stale, stale))  # a day ago: outside the freshness window
    # the newest arrival, and deliberately last in id order so a cursor would reach it last
    store.write_yaml(f"chats/oc_1/messages/{month}/om_999.yaml", {"message_id": "om_999"})

    for i in range(3):
        asked.clear()
        run(sync_module.recheck_messages(store, Progress(), batch=2, budget=2))
        assert "om_999" in asked, f"round {i}: the newest message waited for its turn in the cycle: {asked}"


def test_a_recall_recorded_by_an_earlier_slice_survives_the_next(tmp_path, monkeypatch):
    """`recalled.yaml` is the only record that a message is gone -- the local copy is kept
    deliberately. A bounded pass only sees its own ids, so rewriting the file from what came
    back erases every recall found before it. And a message that answers again must lose its
    record, or it reads as recalled forever."""
    from lark_fs import sync as sync_module

    present: set[str] = set()

    async def fake_run(*argv, **_):
        chunk = argv[argv.index("--message-ids") + 1].split(",")
        return {"messages": [{"message_id": m, "update_time": "1", "body": {}} for m in chunk if m in present]}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    month = datetime.now(UTC).strftime("%Y-%m")
    old = datetime.now(UTC).timestamp() - 86400
    for i in range(4):
        rel = f"chats/oc_1/messages/{month}/om_{i:03}.yaml"
        store.write_yaml(rel, {"message_id": f"om_{i:03}"})
        utime(store.root / rel, (old, old))

    seen: set[str] = set()
    for _ in range(3):  # enough rounds for the sweeping half to reach every id
        run(sync_module.recheck_messages(store, Progress(), batch=2, budget=2))
        seen |= {r["message_id"] for r in store.read_yaml_rows("recalled.yaml")}
    assert seen == {"om_000", "om_001", "om_002", "om_003"}, f"a recall found by an earlier slice was erased by a later one: {seen}"

    present = {"om_000", "om_001", "om_002", "om_003"}
    for _ in range(3):
        run(sync_module.recheck_messages(store, Progress(), batch=2, budget=2))
    assert store.read_yaml_rows("recalled.yaml") == [], "messages that came back still read as recalled"


def test_a_roster_on_disk_is_not_a_roster_forever(tmp_path, monkeypatch):
    """People join, leave and are renamed. `members.yaml` existing said nothing about any
    of that, so the first roster ever fetched was the permanent one -- and the pass reported
    "rosters up to date" while it was wrong."""
    from lark_fs import sync as sync_module

    asked: list[str] = []

    async def fake_run(*argv, **_):
        asked.append(argv[argv.index("--chat-id") + 1])
        return {"users": [{"member_id": "ou_1", "name": "after"}]}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("chats/oc_1/members.yaml", {"users": [{"member_id": "ou_1", "name": "before"}]})

    run(sync_module.sync_chat_meta(store, Progress(), {"oc_1"}))
    assert asked == ["oc_1"], "a roster that had never been refreshed was skipped"
    assert store.read_yaml("chats/oc_1/members.yaml")["users"][0]["name"] == "after"

    asked.clear()
    run(sync_module.sync_chat_meta(store, Progress(), {"oc_1"}))
    assert asked == [], "the clock did not hold; every sync would re-fetch every roster"


def test_a_meeting_still_waiting_for_its_minute_is_asked_again(tmp_path):
    """`minute_token` appears only once the recording is processed, after the meeting ends,
    so a detail fetched while it was running never has one. But a meeting the API has
    already answered for -- 133 of 842 have no minute at all and it says so in `hint` --
    must not be asked forever."""
    from lark_fs.sync import TENANT_TZ, _meeting_detail_is_due

    store = Store(tmp_path)
    just_ended = (datetime.now(TENANT_TZ) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M")
    long_over = (datetime.now(TENANT_TZ) - timedelta(days=90)).strftime("%Y-%m-%d %H:%M")

    store.write_yaml("meetings/m1/detail.yaml", {"end_time": just_ended})
    assert _meeting_detail_is_due(store, "m1") is True, "a meeting that just ended was frozen without its minute"

    store.write_yaml("meetings/m2/detail.yaml", {"end_time": long_over})
    assert _meeting_detail_is_due(store, "m2") is False, "a meeting that will never have a minute is asked forever"

    store.write_yaml("meetings/m3/detail.yaml", {"end_time": just_ended, "minute_token": "obc_1"})
    assert _meeting_detail_is_due(store, "m3") is False, "a detail that already has its minute was re-fetched"

    assert _meeting_detail_is_due(store, "m4") is True, "a meeting with no detail at all was skipped"


def test_a_partial_chat_listing_does_not_shrink_the_sweep(tmp_path, monkeypatch):
    """`+chat-list` failing on its third page still returns two pages, and taking that as
    the answer drops every chat it had not reached -- for the whole run, message sweep
    included, with nothing said about it. A chat mirrored once still exists whether or not
    this listing reached it, so the disk is the floor and not a fallback."""
    from lark_fs.sync import _list_chats

    async def half_a_listing(*_a, **_k):
        yield {"chat_id": "oc_new"}
        raise cli.LarkError(["im", "+chat-list"], {"error": {"code": 99991400}})

    monkeypatch.setattr(cli, "paginate", half_a_listing)
    store = Store(tmp_path)
    store.write_yaml("chats/oc_old/meta.yaml", {"chat_id": "oc_old"})

    assert run(_list_chats(store)) == {"oc_new", "oc_old"}, "a chat already on disk was dropped by a listing that never reached it"


BUG = "a real bug"


def test_a_real_failure_is_not_reported_as_an_interruption(monkeypatch):
    """Every exception out of the sync was rewritten to SyncAbortedError, and `main` prints
    "interrupted; rerun to resume" for that -- the one message that says nothing is wrong.
    A missing scope, a crash and a stopped sync all looked identical.

    Only the TTY path does the rewriting, so the test has to take it: a non-tty run goes
    to `run_plain`, where the exception propagates on its own and proves nothing."""
    from prompt_toolkit.application import create_app_session
    from prompt_toolkit.input import create_pipe_input
    from prompt_toolkit.output import DummyOutput

    from lark_fs.tui import run_with_tui

    monkeypatch.setattr("lark_fs.tui.stderr", type("T", (), {"isatty": lambda _s: True})())

    async def explode(_p):
        raise RuntimeError(BUG)

    async def main():
        with create_pipe_input() as pipe, create_app_session(input=pipe, output=DummyOutput()):
            return await run_with_tui(explode, ["messages"])

    with pytest.raises(RuntimeError, match=BUG):
        run(main())


def test_a_quota_stop_does_not_read_as_a_rerunnable_interruption(monkeypatch):
    """`99991403` is the tenant's *monthly* allowance, shared by every custom app in it and
    cleared only on the 1st. Every collection catches LarkError, so the one that hits it
    swallows it and the run ends at the next checkpoint as a plain abort -- which prints
    "rerun to resume", the one thing that cannot work. Rerunning also burns next month."""
    from lark_fs import cli as cli_module

    monkeypatch.setattr(cli_module.Aborted, "flag", False)
    monkeypatch.setattr(cli_module.Aborted, "reason", "")

    async def quota_spent(*_a, **_k):
        return b'{"ok": false, "error": {"code": 99991403, "message": "quota"}}', b""

    class Proc:
        returncode = 1
        communicate = quota_spent

    monkeypatch.setattr(cli_module, "create_subprocess_exec", lambda *_a, **_k: _done(Proc()))

    with pytest.raises(cli.LarkError):
        run(cli_module.run("im", "+chat-list"))

    assert cli_module.Aborted.flag is True, "the run kept spending against an exhausted quota"
    assert "monthly" in cli_module.Aborted.reason, f"the stop cannot be told from a keystroke: {cli_module.Aborted.reason!r}"


async def _done(value):
    return value


def test_a_media_row_can_be_opened_in_feishu():
    """The design asked for media stored as URLs. Lark gives an attachment no address of
    its own -- a key is only usable through `+messages-resources-download` -- but the
    message that carried it has an applink, and that is already on the message. So a
    filename found by grep leads somewhere instead of dead-ending at an opaque key."""
    row = _index_media(
        {
            "message_id": "om_1",
            "chat_id": "oc_1",
            "msg_type": "file",
            "content": '<file key="file_v3_001_abc" name="notes.md">',
            "message_app_link": "https://applink.feishu.cn/client/chat/open?openChatId=oc_1&position=42",
        }
    )[0]
    assert row["link"] == "https://applink.feishu.cn/client/chat/open?openChatId=oc_1&position=42", f"the media row cannot be opened: {row}"
    assert row["key"] == "file_v3_001_abc" and row["name"] == "notes.md"


def test_a_month_already_walked_is_not_walked_again_every_sync(tmp_path, monkeypatch):
    """History does not grow backwards, so re-listing every month on every sync spends
    requests to learn nothing: 102 of one sync's 435 requests were the minutes walk and 57
    the meetings walk, 36% of the run. Only this month and the last can still change. The
    full walk stays on a clock, for a late entry that lands further back."""
    from lark_fs import sync as sync_module

    windows: list[str] = []

    async def listing(*argv, **_k):
        windows.append(argv[argv.index("--start") + 1])
        return
        yield

    monkeypatch.setattr(cli, "paginate", listing)
    monkeypatch.setattr(cli, "run", lambda *_a, **_k: _done({}))
    store = Store(tmp_path)
    store.write_yaml("minutes/obc_1/meta.yaml", {"display_info": "开始时间: 2024.01.15 10:00"})

    run(sync_module.sync_minutes(store, Progress(), full=True))
    assert min(windows).startswith("2023-12"), f"a full walk must reach the start of history: {min(windows)}"
    assert len(windows) > 12, f"a full walk from 2023-12 is more than a year of windows: {len(windows)}"

    windows.clear()
    run(sync_module.sync_minutes(store, Progress(), full=False))
    assert len(windows) == 2, f"a recent walk is this month and the last, not {len(windows)} windows"


def test_the_full_walk_is_what_claims_the_clock(tmp_path, monkeypatch):
    """A recent walk has not covered history, so it must not let the full one coast."""
    from lark_fs import sync as sync_module

    async def nothing(*_a, **_k):
        return
        yield

    monkeypatch.setattr(cli, "paginate", nothing)
    monkeypatch.setattr(cli, "run", lambda *_a, **_k: _done({}))
    store = Store(tmp_path)

    run(sync_module.sync_minutes(store, Progress(), full=False))
    assert swept_recently(store, "minutes", 6) is False, "a two-month walk claimed the window a full one is due for"

    run(sync_module.sync_minutes(store, Progress(), full=True))
    assert swept_recently(store, "minutes", 6) is True


def test_the_cursor_does_not_advance_past_the_minute_it_read(tmp_path, monkeypatch):
    """`create_time` has minute resolution and `--start` includes that minute, so a cursor
    moved past the last message read would skip everything that arrives later in the same
    minute. Those are common: 861 same-minute groups in 4000 files sampled off the real
    store, and asking with a message's own create_time returns that message.

    The comment here used to claim the cursor was stored "one second past the last message";
    the code never did that, and doing it would lose messages silently and permanently."""
    from lark_fs import sync as sync_module

    starts: list[str] = []
    batches = [
        [{"message_id": "om_1", "create_time": "2026-08-03 11:17", "chat_id": "oc_1"}],
        [{"message_id": "om_2", "create_time": "2026-08-03 11:17", "chat_id": "oc_1"}],  # same minute, arrived later
    ]

    def paginate(*argv, **_k):
        starts.append(argv[argv.index("--start") + 1] if "--start" in argv else "")
        return _aiter(batches.pop(0) if batches else [])

    monkeypatch.setattr(cli, "paginate", paginate)
    store = Store(tmp_path)

    run(sync_module.sync_messages(store, Progress(), chat_ids={"oc_1"}))
    assert store.cursors["chats"]["oc_1"] == "2026-08-03 11:17"

    run(sync_module.sync_messages(store, Progress(), chat_ids={"oc_1"}))
    assert starts[1] == "2026-08-03 11:17", f"the second run asked past the minute it had read: {starts}"
    assert store.exists("chats/oc_1/messages/2026-08/om_2.yaml"), "a message that arrived later in the same minute was never fetched"


def test_one_pass_saving_its_cursor_does_not_undo_another(tmp_path):
    """Cursors are read into memory once, at construction, and `save_cursors` writes the
    whole dict back. That is fine for the sync, which owns the file for its run, and wrong
    for anything holding a Store beside it: the daemon keeps one from startup and hands it
    to `recheck_messages` while `sync_all` runs on its own, and `reindex` makes a third and
    takes minutes. A full save from the stale one erases every advance the others made --
    chat cursors regress, and `threads_incomplete` registrations disappear.

    Merging is not the fix: the repair queue has to be able to shrink, and a merge would
    restore every entry a repair had just cleared."""
    daemon = Store(tmp_path)  # loaded at startup, before the sync below runs
    daemon.cursors["recheck_after"] = "om_000"
    daemon.save_cursor("recheck_after")

    sweep = Store(tmp_path)
    sweep.cursors["chats"] = {"oc_1": "2026-08-03 11:17"}
    sweep.cursors["threads_incomplete"] = {"omt_1": "oc_1"}
    sweep.save_cursors()

    daemon.cursors["recheck_after"] = "om_500"  # the daemon's next tick, still holding its snapshot
    daemon.save_cursor("recheck_after")

    after = Store(tmp_path).cursors
    assert after["recheck_after"] == "om_500", "the tier's own cursor did not persist"
    assert after["chats"] == {"oc_1": "2026-08-03 11:17"}, "the sweep's chat cursors were erased by another pass"
    assert after["threads_incomplete"] == {"omt_1": "oc_1"}, "a thread left truncated at the inline cap lost the only record that would fix it"


def test_a_repaired_thread_can_leave_the_queue(tmp_path):
    """The counterpart: the queue has to shrink. A save that merged with disk would put
    back every entry a repair had just cleared, and the repair would run forever."""
    store = Store(tmp_path)
    store.cursors["threads_incomplete"] = {"omt_1": "oc_1", "omt_2": "oc_1"}
    store.save_cursors()

    store.cursors["threads_incomplete"].pop("omt_1")
    store.save_cursors()

    assert Store(tmp_path).cursors["threads_incomplete"] == {"omt_2": "oc_1"}, "a repaired thread came back onto the queue"


def test_the_recheck_tier_does_not_clobber_the_sweep_it_runs_beside(tmp_path, monkeypatch):
    """The daemon builds one Store at startup and hands it to this tier, while `sync_all`
    runs on its own. This tier grew a cursor of its own -- and saving it the obvious way
    wrote the startup snapshot back over every chat cursor the sweep had advanced since,
    and over every `threads_incomplete` registration, every 30 minutes."""
    from lark_fs import sync as sync_module

    async def unchanged(*argv, **_):
        chunk = argv[argv.index("--message-ids") + 1].split(",")
        return {"messages": [{"message_id": m} for m in chunk]}

    monkeypatch.setattr(cli, "run", unchanged)
    daemon = Store(tmp_path)  # the daemon's Store, loaded before the sweep below
    month = datetime.now(UTC).strftime("%Y-%m")
    for i in range(4):
        daemon.write_yaml(f"chats/oc_1/messages/{month}/om_{i:03}.yaml", {"message_id": f"om_{i:03}"})

    sweep = Store(tmp_path)
    sweep.cursors["chats"] = {"oc_1": "2026-08-03 11:17"}
    sweep.cursors["threads_incomplete"] = {"omt_1": "oc_1"}
    sweep.save_cursors()

    run(sync_module.recheck_messages(daemon, Progress(), batch=2, budget=2))

    after = Store(tmp_path).cursors
    assert after.get("recheck_after"), "the tier's own cursor was not persisted"
    assert after.get("chats") == {"oc_1": "2026-08-03 11:17"}, "the sweep's chat cursors were rolled back to the daemon's startup snapshot"
    assert after.get("threads_incomplete") == {"omt_1": "oc_1"}, "threads left truncated at the inline cap lost the only record that would fix them"


def test_a_thread_meta_never_reports_fewer_replies_than_it_holds(tmp_path):
    """A listing inlines at most 50 replies, and a repair may already have written more, so
    the inline view is a floor and not the truth. Reporting it as the truth walked a
    thread's count backwards -- one on the real store said `replies: 0` beside five reply
    files -- and reset `has_more`, re-queueing a repair that had already finished."""
    from lark_fs.sync import _write_thread

    store = Store(tmp_path)
    at = "chats/oc_1/threads/omt_1"
    root = {"message_id": "om_root", "thread_id": "omt_1", "chat_id": "oc_1", "create_time": "2026-08-04 22:19"}

    _write_thread(store, "oc_1", "omt_1", {**root, "thread_replies": [{"message_id": f"om_r{i}", "create_time": f"2026-08-04 22:2{i}"} for i in range(5)]})
    assert store.read_yaml(f"{at}/meta.yaml")["replies"] == 5

    _write_thread(store, "oc_1", "omt_1", root)  # sighted again with nothing inlined
    meta = store.read_yaml(f"{at}/meta.yaml")
    assert meta["replies"] == 5, f"the count walked backwards past what is on disk: {meta}"
    assert meta["last_reply"] == "2026-08-04 22:24", f"the newest reply was forgotten: {meta}"


def test_an_empty_repair_leaves_the_record_it_found(tmp_path, monkeypatch):
    """`repair_thread` wrote `replies: 0, has_more: False` for a successful call that
    returned nothing -- over a count that was right, orphaning the files it described. Its
    caller reads `[]` as failure and leaves the thread queued, so the pair meant a thread
    refetched on every run whose record said it held nothing."""
    from lark_fs import sync as sync_module

    monkeypatch.setattr(cli, "paginate", lambda *_a, **_k: _aiter([]))
    store = Store(tmp_path)
    at = "chats/oc_1/threads/omt_1"
    store.write_yaml(f"{at}/meta.yaml", {"thread_id": "omt_1", "root_message_id": "om_root", "replies": 5, "has_more": True, "last_reply": "2026-08-04 22:24"})

    assert run(sync_module.repair_thread(store, "omt_1", "oc_1")) == []
    meta = store.read_yaml(f"{at}/meta.yaml")
    assert meta["replies"] == 5, f"an empty answer erased a count that was right: {meta}"
    assert meta["has_more"] is True, "a thread that was never actually repaired was marked complete"


def test_one_collection_failing_is_not_the_sync_failing(tmp_path, monkeypatch):
    """`gather` without `return_exceptions` propagates the first error out of `sync_all`,
    past `store.save_cursors()`, and out of `watch` -- which has nothing above it. So one
    collection raising stops the daemon and discards the sweep clocks of every collection
    that had already finished. `sync_profiles` raised on `contact:user:search`, a scope a
    tenant may simply not grant, and a real install hit exactly that."""
    from lark_fs import sync as sync_module

    ran: list[str] = []

    def stub(name: str, *, boom: bool = False):
        async def collection(*_a, **_k):
            if boom:
                raise cli.LarkError([name], {"error": {"code": 99991672, "message": "missing scope"}})
            ran.append(name)

        return collection

    monkeypatch.setattr(sync_module, "_learn_tenant", lambda _s: None)
    monkeypatch.setattr(sync_module, "_list_chats", stub("roster"))
    monkeypatch.setattr(sync_module, "sync_profiles", stub("profiles", boom=True))
    for name in ("sync_messages", "sync_chat_meta", "sync_docs", "sync_minutes", "sync_meetings", "sync_bases", "sync_wiki", "sync_attachments"):
        monkeypatch.setattr(sync_module, name, stub(name))

    p_ = Progress()
    store = run(sync_module.sync_all(tmp_path, p_))

    assert "sync_minutes" in ran and "sync_bases" in ran, f"a failing collection took the others with it: {ran}"
    assert store.cursors is not None, "the run never reached the point where it saves"
    # and it has to be visible: both renderers walk the names they were given, so a row
    # outside that list is never drawn -- reporting there is the silent failure again
    assert p_.rows["profiles"]["state"] == "error", f"the failure was not reported on the row that gets drawn: {sorted(p_.rows)}"
    assert set(p_.rows) <= set(sync_module.ALL) | {"roster"}, f"a row nothing renders: {sorted(set(p_.rows) - set(sync_module.ALL))}"


def test_a_deliberate_stop_still_stops(tmp_path, monkeypatch):
    """The counterpart: ctrl-c and an exhausted monthly quota are not one collection's
    problem, and swallowing them would keep the sync running against a spent quota."""
    from lark_fs import sync as sync_module

    async def stopped(*_a, **_k):
        raise cli.SyncAbortedError

    async def fine(*_a, **_k):
        return None

    monkeypatch.setattr(sync_module, "_learn_tenant", lambda _s: None)
    monkeypatch.setattr(sync_module, "_list_chats", fine)
    monkeypatch.setattr(sync_module, "sync_bases", stopped)
    for name in ("sync_messages", "sync_chat_meta", "sync_profiles", "sync_docs", "sync_minutes", "sync_meetings", "sync_wiki", "sync_attachments"):
        monkeypatch.setattr(sync_module, name, fine)

    with pytest.raises(cli.SyncAbortedError):
        run(sync_module.sync_all(tmp_path, Progress()))


def test_reindex_notices_a_file_it_cannot_read_anywhere_in_the_store(tmp_path):
    """The scan rebuilds projections from messages, so messages are all it looked at. Five
    `docs/*/comments.yaml` -- written by a serializer that could not round-trip a block
    scalar whose first line was blank and more indented than its content -- sat unreadable
    with nothing in the mirror to say so. A message has `messages_unreadable` to get it
    refetched; these had no equivalent, so noticing is the whole protection."""
    store = Store(tmp_path)
    month = datetime.now(UTC).strftime("%Y-%m")
    store.write_yaml(f"chats/oc_1/messages/{month}/om_1.yaml", {"message_id": "om_1", "content": ""})
    # the exact shape, from `docs/Ngo1dvaFLoSkUYxfHP5cGu5QnPb/comments.yaml`: a block scalar
    # opened without an indentation indicator whose first line is blank and deeper than the
    # line after it, which ends the block early and leaves the rest as a stray mapping
    store.write("docs/tok_1/comments.yaml", "items:\n  - text: |-\n         \n      邀请你进项目了\n")

    assert reindex(tmp_path)["damaged"] == 1, "a file the mirror cannot read back went unreported"

    store.write_yaml("docs/tok_1/comments.yaml", {"items": [{"text": "   \n邀请你进项目了"}]})
    assert reindex(tmp_path)["damaged"] == 0, "a file the current serializer wrote was called damaged"


def test_comments_are_asked_for_even_when_the_body_has_settled(tmp_path, monkeypatch):
    """The comments fetch sat inside the body pass, which only runs for a document whose
    body is stale. So one transient failure there was permanent: the body settled, the pass
    stopped visiting, and the comments were never asked for again. 258 documents on the real
    store -- 8% of the corpus -- had a body and no record of ever having been asked."""
    from lark_fs import sync as sync_module

    calls: list[str] = []

    async def fake_run(*argv, **_):
        calls.append(argv[1])
        if argv[1] == "+fetch":
            return {"document": {"content": "body"}}
        return {"items": []}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("wiki/s1/nodes.yaml", [{"node_token": "n1", "obj_token": "tok1", "obj_type": "docx", "title": "settled"}])
    store.write("docs/tok1/content.md", "body")  # already fetched, and nothing says it moved

    run(sync_module.sync_docs(store, Progress(), search=False))

    assert "+list-comments" in calls, f"a document whose body had settled was never asked for comments: {calls}"
    assert "+fetch" not in calls, f"the body was re-fetched to reach the comments beside it: {calls}"
    assert store.exists("docs/tok1/comments.yaml"), "an empty answer left no record, so the next run cannot tell it from never having asked"

    calls.clear()
    run(sync_module.sync_docs(store, Progress(), search=False))
    assert calls == [], f"a document with both records was visited again: {calls}"


def test_a_document_this_run_did_not_rediscover_is_still_finished(tmp_path, monkeypatch):
    """A search is a ranked slice, so what a run is handed is not what the mirror knows. A
    document found by an earlier run and not returned by this one was never visited again,
    whatever state it had been left in -- 15 on the real store sat with a body and no record
    of ever having been asked for comments, waiting for a slice that might never come."""
    from lark_fs import sync as sync_module

    asked: list[str] = []

    async def fake_run(*argv, **_):
        asked.append(argv[argv.index("--token") + 1] if "--token" in argv else argv[1])
        return {"items": []}

    async def fake_paginate(*argv, **_k):  # this run's search returns nothing
        if argv[1] == "+list-comments":
            asked.append(argv[argv.index("--token") + 1])
        for item in ():
            yield item

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "paginate", fake_paginate)
    store = Store(tmp_path)
    store.write_yaml("docs/tok_forgotten/meta.yaml", {"token": "tok_forgotten", "doc_types": "DOCX"})
    store.write("docs/tok_forgotten/content.md", "a body from an earlier run")

    run(sync_module.sync_docs(store, Progress(), search=True))

    assert "tok_forgotten" in asked, f"a document the mirror already had was left unfinished: {asked}"
    assert store.exists("docs/tok_forgotten/comments.yaml")


def test_a_wiki_only_document_is_still_addressable_after_the_search_that_found_it(tmp_path, monkeypatch):
    """`comment_type` is decided at discovery, from the alias table -- a WIKI hit whose node
    token no node list resolves is addressable only as `wiki`, and that is not derivable
    from the record afterwards. It was kept in memory and never written down, so the run
    that met the document again through the directory instead of a search hit re-derived
    `docx` from `doc_types`, got 1069307, and filed it as having no comments. 260 documents
    in the mirror are in that group; the first one tried this way had four comments."""
    from lark_fs import sync as sync_module

    asked: list[tuple[str, str]] = []

    async def fake_run(*_argv, **_):
        return {"document": {"content": "body"}}

    async def fake_paginate(*argv, **_k):
        if argv[1] == "+search":
            yield {"entity_type": "WIKI", "title_highlighted": "stranded", "result_meta": {"token": "node_orphan", "doc_types": "DOCX", "url": ""}}
            return
        asked.append((argv[argv.index("--token") + 1], argv[argv.index("--type") + 1]))
        raise cli.LarkError(list(argv), {"error": {"code": 99991400}})  # transient: leaves no record behind

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "paginate", fake_paginate)
    store = Store(tmp_path)
    run(sync_module.sync_docs(store, Progress(), queries=["q"]))
    assert asked == [("node_orphan", "wiki")], f"discovery itself must address it as a wiki node: {asked}"

    asked.clear()
    run(sync_module.sync_docs(store, Progress(), search=False))
    assert asked == [("node_orphan", "wiki")], f"the next run only has the directory to go on, and must reach the same document: {asked}"


def test_a_kind_the_comments_endpoint_cannot_name_is_not_asked_again(tmp_path, monkeypatch):
    """The mirror holds one mindnote, and no `--type` reaches it: `mindnote` is not a value
    the endpoint accepts, `docx` gets 1069307, and `wiki` gets lark-cli's own refusal before
    the request is even sent. That last one carries no API code, so it read as transient and
    the document was asked again on every run, forever."""
    from lark_fs import sync as sync_module

    payload = {"error": {"type": "validation", "subtype": "invalid_argument", "message": 'wiki resolved to "mindnote", but comments list only supports doc, docx, sheet, file, slides, bitable, and apps', "param": "--token"}}
    calls = 0

    async def fake_run(*argv, **_):
        nonlocal calls
        calls += 1
        raise cli.LarkError(list(argv), payload)

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("docs/mind/meta.yaml", {"token": "mind", "doc_types": "MINDNOTE", "entity_type": "WIKI", "comment_type": "wiki"})
    store.write("docs/mind/.nobody", "unsupported")

    run(sync_module.sync_docs(store, Progress(), search=False))
    assert calls == 1, "the body is already known to be unexportable, so only comments were due"
    assert (tmp_path / "docs/mind/.nocomments").exists(), "a refusal the endpoint will repeat verbatim has to be remembered"

    run(sync_module.sync_docs(store, Progress(), search=False))
    assert calls == 1, f"and it must not be asked a second time: {calls} calls"


def test_a_document_with_more_comments_than_one_page_keeps_all_of_them(tmp_path, monkeypatch):
    """`+list-comments` was a single call, so it stopped at whatever one page holds. Nine
    documents in the mirror sat at exactly 50 items with `has_more` set -- the busiest ones,
    which are the ones worth having. Asking the first of them for its second page returned
    33 more comments that had never been written down."""
    from lark_fs import sync as sync_module

    pages = {None: ([{"comment_id": str(i)} for i in range(50)], "p2"), "p2": ([{"comment_id": str(i)} for i in range(50, 83)], None)}

    async def fake_run(*argv, **_):
        if argv[1] == "+fetch":
            return {"document": {"content": "body"}}
        items, nxt = pages[argv[argv.index("--page-token") + 1] if "--page-token" in argv else None]
        return {"items": items, "has_more": nxt is not None, "page_token": nxt or ""}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("docs/busy/meta.yaml", {"token": "busy", "doc_types": "DOCX"})
    store.write("docs/busy/content.md", "a body from an earlier run")

    run(sync_module.sync_docs(store, Progress(), search=False))
    assert len(store.read_yaml("docs/busy/comments.yaml")["items"]) == 83, "the second page was dropped"


def test_comments_are_asked_about_again_once_the_record_is_old(tmp_path):
    """Nothing tells the mirror a comment was posted: commenting does not touch the body's
    `update_time`, and 26 of the 403 documents that have comments carry one newer than it.
    So a record written once stayed as it was forever -- and the two that matter move at
    different speeds, since a document that already has comments is where the next one
    appears while 87% of the corpus has none and never will."""
    from lark_fs.sync import COMMENT_EMPTY_HOURS, COMMENT_HOURS, _doc_wants_comments

    store = Store(tmp_path)
    for token, record in (("busy", {"count": 1, "items": [{"comment_id": "1"}]}), ("quiet", {"count": 0, "items": []})):
        store.write_yaml(f"docs/{token}/meta.yaml", {"token": token})
        store.write_yaml(f"docs/{token}/comments.yaml", record)
    assert not _doc_wants_comments(store, "busy"), "a record written moments ago is current"

    def age(token: str, hours: float):
        path = tmp_path / f"docs/{token}/comments.yaml"
        utime(path, (path.stat().st_atime, datetime.now(UTC).timestamp() - hours * 3600 - 60))

    age("busy", COMMENT_HOURS)
    age("quiet", COMMENT_HOURS)
    assert _doc_wants_comments(store, "busy"), "a document with comments is where the next one appears"
    assert not _doc_wants_comments(store, "quiet"), "asking every empty document daily is 87% of the corpus for nothing"

    age("quiet", COMMENT_EMPTY_HOURS)
    assert _doc_wants_comments(store, "quiet"), "but a document without comments can still get its first"

    store.write("docs/quiet/.nocomments", "")
    assert not _doc_wants_comments(store, "quiet"), "a permanent verdict outranks any clock"


def test_a_thousand_members_is_not_a_whole_roster(tmp_path, monkeypatch):
    """`--page-all` reads as "every page" and is not: lark-cli's `--page-limit` defaults to
    ten, and at the max page size that stops at a thousand. Four chats on this store sat at
    exactly 1000 members with `has_more` still set; the largest of them has 3296."""
    from lark_fs import sync as sync_module

    argvs: list[tuple[str, ...]] = []

    async def fake_run(*argv, **_):
        argvs.append(argv)
        return {"users": [], "bots": []}

    monkeypatch.setattr(cli, "run", fake_run)
    store = Store(tmp_path)
    store.write_yaml("chats/oc_1/meta.yaml", {"chat_id": "oc_1"})
    run(sync_module.sync_chat_meta(store, Progress(), {"oc_1"}))

    assert argvs, "no roster was asked for at all"
    for argv in argvs:
        assert "--page-all" in argv and argv[argv.index("--page-limit") + 1] == "0", f"the page cap is still in place: {argv}"


def test_a_bitable_the_doc_pass_already_found_is_mirrored_too(tmp_path, monkeypatch):
    """Bases were discovered by one `+search` with an empty query -- a single ranked slice,
    which returned 11 of the 178 bitables the document pass has on disk. That pass asks 14
    queries and records what each hit was, so the type is already written down; a title that
    merely contains the word is not, which is why the field is matched and not the text."""
    from lark_fs import sync as sync_module

    listed: list[str] = []

    async def fake_run(*argv, **_):
        listed.append(argv[argv.index("--base-token") + 1])
        return {"tables": []}

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "paginate", lambda *_a, **_k: _aiter([]))  # the empty query finds nothing this time
    store = Store(tmp_path)
    store.write_yaml("docs/from_search/meta.yaml", {"token": "from_search", "doc_types": "BITABLE"})
    store.write_yaml("docs/from_wiki/meta.yaml", {"token": "from_wiki", "obj_type": "bitable"})
    store.write_yaml("docs/just_talks_about_it/meta.yaml", {"token": "just_talks_about_it", "doc_types": "DOCX", "title": "how to use a bitable"})

    run(sync_module.sync_bases(store, Progress()))
    assert sorted(listed) == ["from_search", "from_wiki"], f"the doc corpus is the record of what a bitable is: {listed}"


def test_a_document_only_ever_linked_in_a_chat_is_still_mirrored(tmp_path):
    """Discovery was a ranked search slice plus the wiki tree, and neither covers what people
    paste at each other: 417 of the 900 documents linked in these chats had never been
    mirrored, 14% again on top of the 3049 that had. The link is free -- the message is
    already on disk -- and its path segment is the type, which `+list-comments` needs.

    A `/wiki/` link carries a node token: measured on five, every one answers 1069307 as
    `docx` and answers as `wiki`. One the node lists resolve is a document the mirror
    already has under its own token, and must not be stored a second time under this one."""
    from lark_fs.sync import _doc_links, _flush_doc_links

    store = Store(tmp_path)
    store.write_yaml("wiki/s1/nodes.yaml", [{"node_token": "nodeknown0000000000000", "obj_token": "obj1000000000000000000", "obj_type": "docx"}])
    store.write_yaml("docs/obj1000000000000000000/meta.yaml", {"token": "obj1000000000000000000"})
    text = "see https://x.feishu.cn/docx/PLAINDOCXTOKEN0000000001 and https://x.feishu.cn/base/BITABLETOKEN00000000001 and https://x.feishu.cn/wiki/nodeknown0000000000000 and https://x.feishu.cn/wiki/nodeorphan000000000000 and https://x.feishu.cn/file/EMBEDDEDATTACHMENT000001"

    _flush_doc_links(store, _doc_links(text))

    assert store.read_yaml("docs/PLAINDOCXTOKEN0000000001/meta.yaml")["doc_types"] == "DOCX"
    assert store.read_yaml("docs/BITABLETOKEN00000000001/meta.yaml")["doc_types"] == "BITABLE", "the URL says what it is, and nothing else does"
    assert store.read_yaml("docs/nodeorphan000000000000/meta.yaml")["comment_type"] == "wiki", "a node token is only addressable as a wiki node"
    assert not store.exists("docs/nodeknown0000000000000/meta.yaml"), "this is `obj1000000000000000000` under another name; mirroring both stores it twice"
    assert not store.exists("docs/EMBEDDEDATTACHMENT000001/meta.yaml"), "a Drive file exports no markdown, and document bodies link 13203 of them"


def test_a_minute_its_meeting_names_is_fetched_even_if_search_missed_it(tmp_path, monkeypatch):
    """`+search` caps at 50 per query, which is why minutes are walked month by month -- but
    a window at its ceiling still hides whatever ranked below it. A recorded meeting names
    its minute in `detail.yaml`, and 191 of the 710 tokens named there had nothing under
    minutes/ at all. That token is everything `+detail` needs."""
    from lark_fs import sync as sync_module

    asked: list[str] = []

    async def fake_run(*argv, **_):
        asked.append(argv[argv.index("--minute-tokens") + 1])
        return {"minutes": []}

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "paginate", lambda *_a, **_k: _aiter([]))  # the search ranks nothing this run
    store = Store(tmp_path)
    store.write_yaml("meetings/m1/detail.yaml", {"meeting_id": "m1", "minute_token": "obcnMinuteToken000001"})
    store.write_yaml("meetings/m2/detail.yaml", {"meeting_id": "m2"})  # never recorded, so nothing to fetch

    run(sync_module.sync_minutes(store, Progress(), since="2026-08-01", full=False))
    assert asked == ["obcnMinuteToken000001"], f"the meeting was the only record that this minute exists: {asked}"
