"""Unit tests for the message view port: every non-card renderer plus the view pipeline.

Fixtures are real tenant data and live outside the repo, so these tests are hermetic --
every API call the renderer makes is served from a fake `get`. Times are asserted in UTC
(the zone the fixtures were recorded in), pinned here so the tests do not depend on the
machine's zone. Interactive cards are covered only through a stubbed .cards module,
since the real one is ported separately.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import types
from typing import Any

os.environ["TZ"] = "UTC"
time.tzset()

from lark_fs.messages import message_view, message_views


class FakeGet:
    """Serves recorded responses by (method, path, params, body); records what was asked."""

    def __init__(self, responses: dict[tuple, dict] | None = None):
        self.responses = responses or {}
        self.calls: list[tuple] = []

    async def __call__(self, method: str, path: str, params: dict | None = None, body: dict | None = None) -> dict:
        key = (method, path, json.dumps(params, sort_keys=True) if params else None, json.dumps(body, sort_keys=True) if body else None)
        self.calls.append(key)
        return self.responses.get(key, {"items": []})


def render(msg_type: str, content: dict | str, **raw: Any) -> dict:
    """One raw API item of the shape GET /im/v1/messages returns."""
    body = content if isinstance(content, str) else json.dumps(content, ensure_ascii=False)
    item = {"message_id": "om_x", "msg_type": msg_type, "body": {"content": body}, "create_time": "1783659613110", "sender": {"id": "ou_1", "sender_name": "Ada"}, "deleted": False, "updated": False}
    item.update(raw)
    return item


def run(coro):
    return asyncio.run(coro)


def content_of(msg_type: str, content: dict | str, mentions: list | None = None, **raw: Any) -> str:
    item = render(msg_type, content, **raw)
    if mentions is not None:
        item["mentions"] = mentions
    return run(message_view(item, get=FakeGet(), reactions=False))["content"]


# -- time formatting (common.FormatTime) ----------------------------------------


def test_create_time_formats_epoch_ms_to_local_minutes():
    view = run(message_view(render("text", {"text": "hi"}), get=FakeGet(), reactions=False))
    assert view["create_time"] == "2026-07-10 05:00"


def test_create_time_zero_or_junk_passes_through_raw():
    for raw_time, expected in (("0", "0"), ("soon", "soon"), ("1783659613", "2026-07-10 05:00")):
        item = render("text", {"text": "x"}, create_time=raw_time)
        assert run(message_view(item, get=FakeGet(), reactions=False))["create_time"] == expected


# -- text and mentions ----------------------------------------------------------


def test_text_resolves_mention_keys_to_at_names():
    mentions = [{"key": "@_user_1", "id": "ou_9", "name": "Ada"}]
    assert content_of("text", {"text": "hi @_user_1"}, mentions=mentions) == "hi @Ada"


def test_text_without_text_field_shows_raw_content():
    assert content_of("text", {"nope": 1}) == '{"nope": 1}'
    assert content_of("text", "not json") == "[Invalid text JSON]"


def test_unknown_msg_type_renders_as_bracketed_type_and_empty_content_stays_empty():
    assert content_of("nonsupport", "This message type is currently not supported") == "[nonsupport]"
    assert content_of("text", "") == ""


def test_mentions_are_simplified_to_key_id_name():
    mentions = [{"key": "@_user_1", "id": {"open_id": "ou_9"}, "name": "Ada", "tenant_key": "t"}]
    view = run(message_view(render("text", {"text": "hi"}, mentions=mentions), get=FakeGet(), reactions=False))
    assert view["mentions"] == [{"key": "@_user_1", "id": "ou_9", "name": "Ada"}]


# -- post (rich text) ------------------------------------------------------------


def test_post_renders_title_blocks_styles_and_links():
    post = {
        "title": "T",
        "content": [
            [{"tag": "text", "text": "bold", "style": ["bold"]}],
            [{"tag": "a", "text": "site", "href": "https://x"}],
            [{"tag": "at", "user_id": "@_all"}],
            [{"tag": "emotion", "emoji_type": "COFFEE"}],
        ],
    }
    assert content_of("post", post) == 'T\n**bold**\n[site](https://x)\n<at user_id="all"></at>\n:Coffee:'.replace(":Coffee:", ":COFFEE:")


def test_post_prefers_content_v2_and_unwraps_locale():
    post = {"zh_cn": {"title": "标题", "content_v2": [[{"tag": "text", "text": "新"}]], "content": [[{"tag": "text", "text": "旧"}]]}}
    assert content_of("post", post) == "标题\n新"


def test_post_empty_renders_rich_text_placeholder_and_files_append_tags():
    post = {"title": "", "content": [], "files": [{"file_key": "f1", "file_name": "a.pdf"}, {"file_key": "f2", "file_name": "assets", "is_folder": False}]}
    assert content_of("post", post) == '[Rich text message]\n<file key="f1" name="a.pdf"/>\n<file key="f2" name="assets"/>'


def test_at_element_without_open_id_prefix_renders_plain_name():
    assert content_of("post", {"content": [[{"tag": "at", "user_id": "u1", "user_name": "Ada"}]]}) == "@Ada"


# -- media and misc renderers ----------------------------------------------------


def test_image_file_audio_video_renderings():
    assert content_of("image", {"image_key": "img_1"}) == "[Image: img_1]"
    assert content_of("image", {}) == "[Image]"
    assert content_of("image", "oops") == "[Invalid image JSON]"
    assert content_of("file", {"file_key": "f", "file_name": "a.pdf"}) == '<file key="f" name="a.pdf"/>'
    assert content_of("file", {"file_key": "f"}) == '<file key="f" name="f"/>'
    assert content_of("file", {}) == "[File]"
    assert content_of("audio", {"file_key": "a", "duration": 3200}) == '<audio key="a" duration="3s"/>'
    assert content_of("audio", {"duration": 3200}) == "[Voice: 3s]"
    assert content_of("audio", {}) == "[Voice]"
    assert content_of("media", {"file_key": "v", "duration": 4600, "image_key": "cover"}) == '<video key="v" name="v" duration="5s" cover_image_key="cover"/>'
    assert content_of("media", {}) == "[Video]"


def test_sticker_share_location_video_chat():
    assert content_of("sticker", {}) == "[Sticker]"
    assert content_of("video_chat", {"topic": "x"}) == "[Video call]"
    assert content_of("share_chat", {"chat_id": "oc_1"}) == "[Chat card: oc_1]"
    assert content_of("share_user", {"user_id": "ou_1"}) == "[User card: ou_1]"
    assert content_of("location", {"name": "HQ"}) == "[Location: HQ]"
    assert content_of("location", {}) == "[Location]"


def test_system_template_placeholders_fill_and_unknown_ones_survive():
    system = {"template": "{from_user} added {to_chatters} to {NewGroup}.", "from_user": ["Ada"], "to_chatters": [], "divider_text": {}}
    assert content_of("system", system) == "Ada added  to {NewGroup}."


def test_system_without_template_is_bracketed_and_blank_template_blanks():
    assert content_of("system", {"template": ""}) == "[System message]"
    assert content_of("system", {"template": " "}) == ""


def test_vote_renders_topic_options_and_closed_status():
    vote = {"topic": "Pick", "options": ["a", "", "b"], "status": 1}
    assert content_of("vote", vote) == "<vote>\nPick\n• a\n• b\n(Closed)\n</vote>"
    assert content_of("vote", {"topic": "Pick", "options": ["a"]}) == "<vote>\nPick\n• a\n</vote>"


def test_todo_renders_task_summary_and_due():
    todo = {"task_id": "t1", "summary": {"title": "Title", "content": [[{"tag": "text", "text": "body"}]]}, "due_time": "1762237200000"}
    out = content_of("todo", todo)
    assert out == '<todo task_id="t1">\nTitle\nbody\nDue: 2025-11-04 06:20:00\n</todo>'
    assert content_of("todo", {"task_id": "t1"}) == '<todo task_id="t1">\ntodo\n</todo>'


def test_calendar_renders_summary_window_and_attrs():
    cal = {"summary": "Standup", "start_time": "1762237200000", "end_time": "1762240800000", "open_event_id": "e1"}
    assert content_of("share_calendar_event", cal) == '<calendar_share open_event_id="e1">\nStandup\n2025-11-04 06:20:00 ~ 2025-11-04 07:20:00\n</calendar_share>'


def test_hongbao_quotes_text_like_go():
    assert content_of("hongbao", {"text": 'a"b'}) == '<hongbao text="a\\"b"/>'
    assert content_of("hongbao", {}) == "<hongbao/>"


def test_xml_bodies_come_out_entity_unescaped():
    # lark-fs unescapes what rendering escaped before writing to disk, so the view
    # carries plain text and `rg` is not defeated by &amp;.
    assert content_of("vote", {"topic": "A&B"}) == "<vote>\nA&B\n</vote>"


# -- view assembly (formatMessageItem) -------------------------------------------


def test_thread_id_wins_over_reply_to():
    item = render("text", {"text": "x"}, thread_id="omt_1", parent_id="om_p")
    view = run(message_view(item, get=FakeGet(), reactions=False))
    assert view["thread_id"] == "omt_1" and "reply_to" not in view


def test_reply_to_used_when_no_thread():
    item = render("text", {"text": "x"}, parent_id="om_p")
    view = run(message_view(item, get=FakeGet(), reactions=False))
    assert view["reply_to"] == "om_p"


def test_update_time_only_when_updated():
    raw = render("text", {"text": "x"}, update_time="1783659613110")
    assert "update_time" not in run(message_view(raw, get=FakeGet(), reactions=False))
    raw["updated"] = True
    assert run(message_view(raw, get=FakeGet(), reactions=False))["update_time"] == "2026-07-10 05:00"


def test_sender_name_becomes_name_and_sender_name_is_dropped():
    item = render("text", {"text": "x"}, sender={"id": "ou_1", "sender_name": "Ada", "sender_i18n_names": {"en_us": "A"}})
    view = run(message_view(item, get=FakeGet(), reactions=False))
    assert view["sender"] == {"id": "ou_1", "name": "Ada", "sender_i18n_names": {"en_us": "A"}}


def test_message_app_link_assembles_chat_link_with_sorted_query():
    item = render("text", {"text": "x"}, chat_id="oc_1", message_position="42")
    assert run(message_view(item, get=FakeGet(), reactions=False))["message_app_link"] == "https://applink.feishu.cn/client/chat/open?openChatId=oc_1&position=42"


def test_message_app_link_assembles_thread_link_with_both_spellings():
    item = render("text", {"text": "x"}, chat_id="oc_1", thread_id="omt_1", message_position="-3", thread_message_position="7")
    link = run(message_view(item, get=FakeGet(), reactions=False))["message_app_link"]
    assert link == "https://applink.feishu.cn/client/thread/open?open_chat_id=oc_1&open_thread_id=omt_1&openchatid=oc_1&openthreadid=omt_1&thread_position=7"


def test_api_provided_app_link_is_preferred():
    item = render("text", {"text": "x"}, chat_id="oc_1", message_position="42", message_app_link="https://given.example/x")
    assert run(message_view(item, get=FakeGet(), reactions=False))["message_app_link"] == "https://given.example/x"


# -- thread inlining (thread.go) --------------------------------------------------

THREAD_PARAMS = {
    "container_id_type": "thread",
    "container_id": "omt_1",
    "sort_type": "ByCreateTimeAsc",
    "page_size": "50",
    "card_msg_content_type": "raw_card_content",
    "with_sender_name": "true",
}


def thread_reply(i: int) -> dict:
    return {
        "message_id": f"om_r{i}",
        "msg_type": "text",
        "body": {"content": json.dumps({"text": f"r{i}"})},
        "create_time": str(1783659613110 + i),
        "sender": {"id": "ou_2", "sender_name": "Bob"},
        "deleted": False,
        "updated": False,
        "thread_id": "omt_1",
    }


def test_thread_host_inlines_replies_and_sender_names():
    root = render("text", {"text": "root"}, thread_id="omt_1")
    get = FakeGet({("GET", "/open-apis/im/v1/messages", json.dumps(THREAD_PARAMS, sort_keys=True), None): {"items": [thread_reply(1), thread_reply(2)], "has_more": False}})
    view = run(message_view(root, get=get, reactions=False))
    assert [r["message_id"] for r in view["thread_replies"]] == ["om_r1", "om_r2"]
    assert view["thread_replies"][0]["sender"] == {"id": "ou_2", "name": "Bob"}
    assert "thread_has_more" not in view


def test_thread_has_more_set_when_fifty_replies_truncate_the_container():
    root = render("text", {"text": "root"}, thread_id="omt_1")
    get = FakeGet({("GET", "/open-apis/im/v1/messages", json.dumps(THREAD_PARAMS, sort_keys=True), None): {"items": [thread_reply(i) for i in range(50)], "has_more": True}})
    view = run(message_view(root, get=get, reactions=False))
    assert len(view["thread_replies"]) == 50
    assert view["thread_has_more"] is True


def test_thread_fetch_failure_flags_thread_replies_error():
    root = render("text", {"text": "root"}, thread_id="omt_1")

    async def broken(*_args: object, **_kwargs: object) -> dict:
        raise RuntimeError("boom")

    view = run(message_view(root, get=broken, reactions=False))
    assert view["thread_replies_error"] is True
    assert "thread_replies" not in view


def test_total_reply_budget_truncates_across_threads():
    # 500 replies is the page-wide budget; the 501st thread's replies are dropped whole.
    hosts = [render("text", {"text": "root"}, message_id=f"om_h{i}", thread_id=f"omt_{i}") for i in range(11)]
    responses = {}
    for i in range(11):
        params = dict(THREAD_PARAMS, container_id=f"omt_{i}")
        responses[("GET", "/open-apis/im/v1/messages", json.dumps(params, sort_keys=True), None)] = {"items": [thread_reply(j) for j in range(50)], "has_more": False}
    views = run(message_views(hosts, get=FakeGet(responses), reactions=False))
    attached = [len(v.get("thread_replies") or []) for v in views]
    assert sum(attached) == 500
    assert attached[-1] == 0


# -- merge_forward (merge.go) ----------------------------------------------------


def test_merge_forward_renders_tree_with_sender_and_indent():
    root = render("merge_forward", "Merged and Forwarded Message", message_id="om_m")
    subs = [
        {"message_id": "om_s1", "msg_type": "text", "body": {"content": json.dumps({"text": "hello"})}, "create_time": "1783659613110", "sender": {"id": "ou_2", "sender_name": "Bob"}},
        {"message_id": "om_s2", "msg_type": "image", "body": {"content": json.dumps({"image_key": "img"})}, "create_time": "1783659614110", "sender": {"id": "ou_2"}},
    ]
    key = ("GET", "/open-apis/im/v1/messages/om_m", json.dumps({"user_id_type": "open_id", "card_msg_content_type": "raw_card_content", "with_sender_name": "true"}, sort_keys=True), None)
    out = run(message_view(root, get=FakeGet({key: {"items": subs}}), reactions=False))["content"]
    assert out == "<forwarded_messages>\n[2026-07-10T05:00:13Z] Bob:\n    hello\n[2026-07-10T05:00:14Z] Bob:\n    [Image: img]\n</forwarded_messages>"


def test_recalled_merge_renders_empty_tree():
    root = render("merge_message", "This message was recalled", message_id="om_m")
    assert run(message_view(root, get=FakeGet(), reactions=False))["content"] == "<forwarded_messages/>"


def test_merge_without_message_id_falls_back_to_id_count():
    assert content_of("merge_forward", {"create_message_ids": ["a", "b"]}, message_id="") == "[Merged forward: 2 messages]"


# -- reactions (reactions.go) ----------------------------------------------------


def test_reactions_attach_counts_and_details_per_message():
    item = render("text", {"text": "x"}, message_id="om_1")
    data = {
        "success_msg_reaction_counts": [{"message_id": "om_1", "reaction_count": [{"reaction_type": "SMILE", "count": "1"}]}],
        "success_msg_reaction_details": [{"message_id": "om_1", "message_reaction_items": [{"reaction_id": "r1", "emoji_type": "SMILE"}]}],
    }
    key = ("POST", "/open-apis/im/v1/messages/reactions/batch_query", None, json.dumps({"queries": [{"message_id": "om_1"}]}, sort_keys=True))
    view = run(message_view(item, get=FakeGet({key: data})))
    assert view["reactions"] == {"counts": [{"reaction_type": "SMILE", "count": "1"}], "details": [{"reaction_id": "r1", "emoji_type": "SMILE"}]}


def test_reactions_can_be_skipped():
    get = FakeGet()
    assert "reactions" not in run(message_view(render("text", {"text": "x"}, message_id="om_1"), get=get, reactions=False))
    assert get.calls == []


def test_reactions_failure_marks_reactions_error():
    async def broken(*_args: object, **_kwargs: object) -> dict:
        raise RuntimeError("quota")

    assert run(message_view(render("text", {"text": "x"}), get=broken))["reactions_error"] is True


def test_reactions_batch_twenty_per_request():
    items = [render("text", {"text": "x"}, message_id=f"om_{i}") for i in range(45)]
    get = FakeGet()
    run(message_views(items, get=get, reactions=True))
    assert len([call for call in get.calls if call[1].endswith("reactions/batch_query")]) == 3  # 20 + 20 + 5


# -- interactive cards (ported separately in .cards) ------------------------------


def test_interactive_delegates_to_render_card(monkeypatch):
    calls: list[tuple] = []
    stub = types.ModuleType("lark_fs.cards")

    def render_card(content: str, mentions: list | None = None, names: dict | None = None) -> str:
        calls.append((content, mentions, names))
        return "<card>rendered</card>"

    stub.render_card = render_card
    monkeypatch.setitem(sys.modules, "lark_fs.cards", stub)
    assert content_of("interactive", '{"json_card": "{}"}') == "<card>rendered</card>"
    assert calls and calls[0][0] == '{"json_card": "{}"}'
