"""Regressions for failures that were silent -- each one shipped and produced plausible output."""

from asyncio import Semaphore, create_task, gather, run, sleep
from datetime import datetime
from itertools import pairwise

from yaml import safe_load

from lark_fs import cli
from lark_fs.attachments import Policy, _pending, _settle
from lark_fs.reindex import reindex
from lark_fs.store import Store
from lark_fs.sync import SLICE_HOURS, STAMP, TENANT_TZ, Progress, _note_tenant, _wiki_aliases, _windows, _write_thread, migrate_threads
from lark_fs.yaml import readable_yaml_dumps


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
    for name, value in NASTY.items():
        text = readable_yaml_dumps({"body": value, "after": "sentinel"})
        loaded = safe_load(text)
        assert loaded["body"] == value, f"{name}: {loaded['body']!r} != {value!r}"
        assert loaded["after"] == "sentinel", f"{name}: the document was restructured"


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
