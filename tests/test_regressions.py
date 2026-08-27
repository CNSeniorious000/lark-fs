"""Regressions for failures that were silent -- each one shipped and produced plausible output."""

from asyncio import Semaphore, create_task, gather, run, sleep
from datetime import UTC, datetime, timedelta
from itertools import pairwise

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


def test_a_partial_sweep_still_claims_its_window(tmp_path, monkeypatch):
    """Measured over one real sweep: of 14 search queries, 4 answered and 10 were refused
    outright. That is the ordinary shape of it, not an incident -- so a stamp that demands
    every query answer never lands, and the pass that was supposed to coast for six hours
    runs on every sync instead. The test is "did it find anything", not "was it flawless"."""
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

    assert swept_recently(store, "docs", 6) is True, "one query answering was enough to sweep, but the window was refused"
