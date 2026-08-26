"""Regressions for failures that were silent -- each one shipped and produced plausible output."""

from asyncio import Semaphore, create_task, gather, run, sleep
from datetime import datetime
from itertools import pairwise
from re import findall

from lark_fs import cli
from lark_fs.attachments import Policy, _pending, _settle
from lark_fs.store import Store
from lark_fs.sync import SLICE_HOURS, STAMP, TENANT_TZ, _note_tenant, _wiki_aliases, _windows
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


def test_yaml_survives_control_characters():
    """U+2028 reads as a line break inside a literal block and silently breaks its indentation."""
    text = readable_yaml_dumps({"body": "first\u2028second\x1b[0m\nthird"})
    assert findall(r"(?m)^\S", text) == ["b"], "a stray break would start a second top-level key"
    assert "\u2028" not in text and "\x1b" not in text


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
