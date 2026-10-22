"""The message view lark-cli renders, minus interactive cards: one raw API item in, one readable dict out.

lark-fs mirrored messages by shelling to `lark-cli` per request; the async API port needs
that same view assembly in Python. This module is a faithful port of lark-cli's
shortcuts/im/convert_lib (content_convert.go, text.go, media.go, misc.go, helpers.go,
reactions.go, thread.go, merge.go) plus the shortcut pipeline order from
im_chat_messages_list.go: prefetch merge_forward children, format each item, resolve
sender names, inline thread replies, then batch reactions. Everything network-shaped
reaches the API through the caller-supplied `get`, so rendering stays testable offline.

Interactive cards are the one exception: they are ported separately in .cards, imported
lazily so this module works before that file lands.
"""

from __future__ import annotations

import asyncio
import html
import json
import math
import re
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote_plus

Get = Callable[..., Awaitable[dict]]  # async (method, path, params=None, body=None) -> the API response's `data` field; raises on failure

# The applink host is per-brand (Feishu vs Lark) and nothing in the API reports it;
# lark-fs mirrors a Feishu tenant, so links are assembled for that brand. lark-cli's
# urlrewrite.Rewrite is an extension hook that is the identity without an extension.
_APP_LINK_HOST = "applink.feishu.cn"

_THREAD_REPLIES_PER_THREAD = 50  # thread.go: max replies fetched per thread root
_THREAD_REPLIES_TOTAL_LIMIT = 500  # thread.go: max total replies across all threads of one page
_REACTIONS_BATCH_MAX = 20  # reactions.go: server-side cap on batch_query queries[]
_MAX_FOLDER_CHILDREN = 10  # misc.go: rendered children per expanded folder
_FETCH_CONCURRENCY = 8  # Go fans these fetches out at 8 in flight

_SYSTEM_PLACEHOLDER = re.compile(r"\{(\w+)}", re.ASCII)  # Go RE2 \w is ASCII-only

_INVALID = {
    "text": "[Invalid text JSON]",
    "rich text": "[Invalid rich text JSON]",
    "image": "[Invalid image JSON]",
    "file": "[Invalid file JSON]",
    "audio": "[Invalid audio JSON]",
    "video": "[Invalid video JSON]",
    "chat card": "[Invalid chat card JSON]",
    "user card": "[Invalid user card JSON]",
    "location": "[Invalid location JSON]",
    "folder": "[Invalid folder JSON]",
    "calendar": "[Invalid calendar JSON]",
    "vote": "[Invalid vote JSON]",
    "hongbao": "[Invalid hongbao JSON]",
    "system message": "[Invalid system message JSON]",
    "todo": "[Invalid todo JSON]",
    "": "[Invalid JSON content]",
}


# -- scalars -------------------------------------------------------------------


def _as_str(value: Any) -> str:
    """Go type assertions (`x.(string)`) silently yield "" for anything else."""
    return value if isinstance(value, str) else ""


def _as_bool(value: Any) -> bool:
    """Go type assertions (`x.(bool)`) silently yield false for anything else."""
    return value if isinstance(value, bool) else False


def _as_number(value: Any) -> float | None:
    """JSON numbers are what Go's `x.(float64)` matches; bool is not a number there."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return float(value)


def _as_list(value: Any) -> list:
    return value if isinstance(value, list) else []


def _go_format_v(value: Any) -> str:
    """Go's %v for the shapes the API actually returns: strings and integers."""
    return "" if value is None else value if isinstance(value, str) else str(value)


def _scan_leading_int(text: str) -> int | None:
    """fmt.Sscanf("%d") reads a leading sign and digits and ignores any tail.

    Go only checks whether the scan landed on zero, so "12abc" formats as 12 while an
    unparseable value falls through to the raw string.
    """
    match = re.match(r"[+-]?\d+", text)
    return int(match.group()) if match else None


def _local(seconds: float) -> datetime:
    """Wall-clock time in the machine's zone, like Go's time.Unix(...).Local().

    The fixtures were recorded with the machine on UTC; a caller in another zone gets the
    same local rendering lark-cli would give there.
    """
    return datetime.fromtimestamp(seconds, tz=UTC).astimezone().replace(tzinfo=None)


def _format_time(value: Any) -> str:
    """common.FormatTime: Unix seconds/ms -> local "YYYY-MM-DD HH:mm".

    The ms-vs-seconds heuristic is a magnitude check (n > 1e12) exactly as lark-cli does
    it, and a zero or unparseable value passes the raw string through instead of becoming
    1970. Times are local, like Go's time.Local -- the fixtures were recorded under UTC.
    """
    s = _go_format_v(value)
    if s == "":
        return ""
    n = _scan_leading_int(s)
    if not n:
        return s
    if n > 10**12:
        n //= 1000
    return _local(n).strftime("%Y-%m-%d %H:%M")


def _format_timestamp(ts: str) -> str:
    """helpers.formatTimestamp: fully numeric Unix seconds/ms -> local "YYYY-MM-DD HH:mm:ss".

    Unlike _format_time this refuses anything that is not a complete integer (calendar
    and todo fields come from card payloads and must not half-parse) and picks ms by
    digit count rather than magnitude.
    """
    if not ts:
        return ""
    try:
        n = int(ts)
    except ValueError:
        return ""
    if n == 0:
        return ""
    if len(ts.lstrip("+-")) >= 13:
        n //= 1000
    return _local(n).strftime("%Y-%m-%d %H:%M:%S")


def _format_merge_timestamp(ts: str) -> str:
    """merge.go FormatMergeForwardTimestamp: ms -> local RFC3339 with offset ("...Z" in UTC)."""
    n = _scan_leading_int(ts) if ts else None
    if not n:
        return "unknown"
    dt = datetime.fromtimestamp(n / 1000, tz=UTC).astimezone()
    offset = dt.strftime("%z")
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + ("Z" if offset == "+0000" else f"{offset[:3]}:{offset[3:]}")


def _card_escape_attr(text: str) -> str:
    """card.go cardEscapeAttr: one-pass escaping for the XML-ish attribute style.

    Order matters: backslashes first so the backslashes introduced by later replacements
    stay single, matching Go's strings.Replacer single pass.
    """
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def _xml_escape_body(text: str) -> str:
    """misc.go xmlEscapeBody: only the three markup characters, never quotes."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_md_link_text(text: str) -> str:
    """A bare bracket in link text would close the Markdown link early."""
    return text.replace("[", "\\[").replace("]", "\\]")


def _go_quote(text: str) -> str:
    """Go's %q (strconv.Quote): hongbao renders its text through Go string quoting."""
    out = ['"']
    for ch in text:
        code = ord(ch)
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif code < 0x20 or code == 0x7F:
            out.append(f"\\x{code:02x}")
        elif not ch.isprintable():
            out.append(f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}")
        else:
            out.append(ch)
    return "".join(out) + '"'


# -- JSON payloads -------------------------------------------------------------


def _parse_json_object(raw: str) -> dict | None:
    """Parse a body.content JSON object; None marks "not a JSON object" like Go's error."""
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _invalid(kind: str) -> str:
    return _INVALID.get(kind, f"[Invalid {kind} JSON]")


# -- mentions and senders ------------------------------------------------------


def _build_mention_key_map(mentions: list) -> dict[str, str]:
    """key -> name for every mention carrying both; bodies address mentions by key."""
    out: dict[str, str] = {}
    for raw in mentions:
        item = raw if isinstance(raw, dict) else {}
        key, name = _as_str(item.get("key")), _as_str(item.get("name"))
        if key and name:
            out[key] = name
    return out


def _resolve_mention_keys(text: str, mention_map: dict[str, str]) -> str:
    """@_user_1 tokens in the body become @name, applied in mention order.

    Go ranges the map in random order; mention order is the deterministic choice and
    also avoids "@_user_1" rewriting the head of "@_user_10".
    """
    for key, name in mention_map.items():
        text = text.replace(key, "@" + name)
    return text


def _extract_mention_open_id(value: Any) -> str:
    """A mention id is either the open_id string or an object holding one."""
    if isinstance(value, str):
        return value
    return _as_str(value.get("open_id")) if isinstance(value, dict) else ""


def _copy_sender(sender: Any) -> Any:
    """AttachSenderNames mutates senders in place in Go; Python callers keep their input intact."""
    return dict(sender) if isinstance(sender, dict) else sender


def _resolve_sender_names(messages: list[dict], cache: dict[str, str] | None) -> dict[str, str]:
    """Harvest each message's server-provided sender_name into the shared open_id -> name cache.

    The read API (with_sender_name=true) is the only name source -- there is deliberately
    no contact lookup fallback, so an unnamed sender stays unnamed downstream.
    """
    names = cache if cache is not None else {}
    for msg in messages:
        sender = msg.get("sender")
        if not isinstance(sender, dict):
            continue
        sid, name = _as_str(sender.get("id")), _as_str(sender.get("sender_name"))
        if sid and name:
            names[sid] = name
    return names


def _attach_sender_names(messages: list[dict], cache: dict[str, str]) -> None:
    """Fold sender_name into `name` and drop the duplicate raw field.

    Senders the server did not name resolve through the cache (merge_forward sub-items,
    thread replies) and otherwise keep no `name`; sender_i18n_names and open_bot_id are
    preserved because they carry the locale names and the bot id alignment.
    """
    for msg in messages:
        sender = msg.get("sender")
        if not isinstance(sender, dict):
            continue
        name = _as_str(sender.get("sender_name")) or cache.get(_as_str(sender.get("id")), "")
        if name:
            sender["name"] = name
        sender.pop("sender_name", None)


# -- app link assembly ---------------------------------------------------------


def _format_float(f: float) -> str:
    """Go's FormatFloat(f, 'f', -1, 64): shortest decimal, never an exponent."""
    if f == int(f):
        return str(int(f))
    text = repr(f)
    return f"{f:.17f}".rstrip("0") if "e" in text or "E" in text else text


def _normalize_message_position(value: Any) -> tuple[str, bool]:
    """Message positions arrive as JSON numbers or numeric strings; both render plain."""
    if value is None or isinstance(value, bool):
        return "", False
    if isinstance(value, int):
        return str(value), True
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "", False
        return _format_float(value), True
    text = value.strip()
    if not text:
        return "", False
    try:
        f = float(text)
    except ValueError:
        return "", False
    if math.isnan(f) or math.isinf(f):
        return "", False
    return _format_float(f), True


def _assemble_message_app_link(m: dict) -> str:
    """Build the deterministic applink fallback when the API returned none.

    Thread links need thread_id + chat_id + a thread position; otherwise a chat link
    needs chat_id + a message position. Query keys come out in Go url.Values.Encode
    order (sorted), and thread links carry both key spellings so PC and mobile clients
    resolve the same URL.
    """
    chat_id = _as_str(m.get("chat_id"))
    thread_id = _as_str(m.get("thread_id"))
    msg_pos, ok_msg = _normalize_message_position(m.get("message_position"))
    thread_pos, ok_thread = _normalize_message_position(m.get("thread_message_position"))
    if thread_id and chat_id and ok_thread:
        query = [
            ("open_chat_id", chat_id),
            ("open_thread_id", thread_id),
            ("openchatid", chat_id),
            ("openthreadid", thread_id),
            ("thread_position", thread_pos),
        ]
        return f"https://{_APP_LINK_HOST}/client/thread/open?" + "&".join(f"{k}={quote_plus(v)}" for k, v in query)
    if chat_id and ok_msg:
        return f"https://{_APP_LINK_HOST}/client/chat/open?openChatId={quote_plus(chat_id)}&position={quote_plus(msg_pos)}"
    return ""


# -- post (rich text) rendering ------------------------------------------------


def _apply_post_style(text: str, raw: Any) -> str:
    """Bold/italic/underline/lineThrough wrap inner-to-outer in fixed order so output is deterministic."""
    if text == "":
        return text
    styles = [s for s in _as_list(raw) if isinstance(s, str)]
    if not styles:
        return text
    if "bold" in styles:
        text = "**" + text + "**"
    if "italic" in styles:
        text = "*" + text + "*"
    if "underline" in styles:
        text = "<u>" + text + "</u>"
    if "lineThrough" in styles:
        text = "~~" + text + "~~"
    return text


def _render_post_elem(el: Any) -> str:
    """One post element inline: text/a/at carry style, emotion becomes :emoji:, md passes through.

    Emotion deliberately skips styling: ":emoji:" is an atomic token, not prose.
    """
    item = el if isinstance(el, dict) else {}
    tag = _as_str(item.get("tag"))
    if tag == "text":
        return _apply_post_style(_as_str(item.get("text")), item.get("style"))
    if tag == "a":
        text, href = _as_str(item.get("text")), _as_str(item.get("href"))
        rendered = f"[{_escape_md_link_text(text)}]({href})" if href and text else href or text
        return _apply_post_style(rendered, item.get("style"))
    if tag == "at":
        user_id = _as_str(item.get("user_id"))
        if user_id in ("@_all", "all"):
            rendered = '<at user_id="all"></at>'
        else:
            name = _as_str(item.get("user_name"))
            rendered = (f'<at user_id="{user_id}">{name}</at>' if user_id.startswith("ou") else "@" + name) if name else "@" + user_id
        return _apply_post_style(rendered, item.get("style"))
    if tag == "emotion":
        emoji = _as_str(item.get("emoji_type"))
        return ":" + emoji + ":" if emoji else ""
    if tag == "md":
        return _as_str(item.get("text"))
    if tag == "img":
        key = _as_str(item.get("image_key"))
        return f"![Image]({key})" if key else "[Image]"
    if tag == "media":
        key = _as_str(item.get("file_key"))
        return f"[Media: {key}]" if key else "[Media]"
    if tag == "code_block":
        lang, code = _as_str(item.get("language")), _as_str(item.get("text"))
        return f"\n```{lang}\n{code}\n```\n" if lang else f"\n```\n{code}\n```\n"
    if tag == "hr":
        return "\n---\n"
    return _as_str(item.get("text"))


def _extract_post_blocks_text(blocks: list) -> str:
    """Plain text of post-style [][]element blocks; empty paragraphs are dropped."""
    lines = []
    for para in blocks:
        rendered = "".join(_render_post_elem(el) for el in _as_list(para))
        if rendered != "":
            lines.append(rendered)
    return "\n".join(lines)


def _select_content_blocks(body: dict) -> list:
    """content_v2 wins when present and non-empty; older messages only carry content."""
    v2 = _as_list(body.get("content_v2"))
    return v2 or _as_list(body.get("content"))


def _unwrap_post_locale(parsed: dict) -> dict | None:
    """Post content is flat (content/title at top level) or wrapped in a locale map.

    The locale fallback order is fixed (zh_cn, en_us, ja_jp) then alphabetical, so one
    payload always renders the same language.
    """
    if "content" in parsed or "title" in parsed:
        return parsed
    for locale in ("zh_cn", "en_us", "ja_jp"):
        value = parsed.get(locale)
        if isinstance(value, dict):
            return value
    for key in sorted(parsed):
        if isinstance(parsed[key], dict):
            return parsed[key]
    return None


# -- folder expansion (misc.go) -------------------------------------------------


def _fallback_folder_line(key: str, name: str) -> str:
    return f'<folder key="{_card_escape_attr(key)}" name="{_card_escape_attr(name)}"/>' if name else f'<folder key="{_card_escape_attr(key)}"/>'


def _write_folder_open(key: str, name: str, child_count: int, force_zero: bool, has_more: bool, self_close: bool) -> str:
    out = f'<folder key="{_card_escape_attr(key)}"'
    if name:
        out += f' name="{_card_escape_attr(name)}"'
    if force_zero or child_count > 0:
        out += f' child_count="{child_count}"'
    if has_more:
        out += ' has_more="true"'
    return out + ("/>" if self_close else ">")


def _render_folder_tree(key: str, name: str, items: list[dict], all_count: int) -> str:
    """One level of folder children, capped at _MAX_FOLDER_CHILDREN with a has_more hint.

    child_count belongs to the root tag (= all_count, or an explicit 0 for a genuinely
    empty folder) so the viewer knows what is not rendered here.
    """
    has_more = all_count > len(items)  # the server may cap items below all_count
    if len(items) > _MAX_FOLDER_CHILDREN:
        has_more = True
    shown = min(len(items), _MAX_FOLDER_CHILDREN)
    out = _write_folder_open(key, name, all_count, shown == 0 and all_count == 0, has_more, shown == 0)
    if shown == 0:
        return out  # self-closing: no odd empty open/close pair
    for item in items[:shown]:
        child_key, child_name = _as_str(item.get("file_key")), _as_str(item.get("name"))
        if _as_bool(item.get("is_folder")):
            out += f'<folder key="{_card_escape_attr(child_key)}" name="{_card_escape_attr(child_name)}"'
            count = item.get("children_count")
            if isinstance(count, int) and not isinstance(count, bool) and count > 0:
                out += f' child_count="{count}"'
            out += "/>"
        else:
            out += f'<file key="{_card_escape_attr(child_key)}" name="{_card_escape_attr(child_name)}"/>'
    return out + "</folder>"


async def _fetch_folder_children_tree(get: Get, folder_key: str, folder_name: str, message_id: str) -> str:
    """Expand one folder level; "" means "tried and failed" and callers degrade to the tag line.

    A folder expansion is a hint for the reader, never a reason to fail the message, so
    every failure mode collapses to the single-line tag.
    """
    try:
        data = await get(
            "GET",
            f"/open-apis/im/v1/files/{folder_key}/folder",
            params={"srctype": "message", "srcid": message_id, "recursive": "false"},
        )
    except Exception as exc:  # noqa: BLE001 - expansion is best-effort by design
        print(f"warning: folder_fetch_failed: {folder_key}: {exc}", file=sys.stderr)
        return ""
    if not isinstance(data, dict):
        print(f"warning: folder_fetch_failed: {folder_key}: empty data", file=sys.stderr)
        return ""
    items = [i for i in _as_list(data.get("items")) if isinstance(i, dict)]
    all_count = data.get("all_count")
    return _render_folder_tree(folder_key, folder_name, items, int(all_count) if isinstance(all_count, int) and not isinstance(all_count, bool) else 0)


async def _prefetch_folder_children(items: list[dict], get: Get | None) -> dict[str, str] | None:
    """misc.go PrefetchFolderChildren: expand every folder in a page up front, keyed for the render loop.

    Folder messages key on message_id; folder attachments inside posts key on
    message_id + NUL + file_key because one post can carry several. A recorded "" value
    means "tried and failed" so the converter degrades without a second GET.
    """
    if get is None or not items:
        return None
    refs: list[tuple[str, str, str, str]] = []  # (cache_key, message_id, file_key, name)
    for item in items:
        msg_type = _as_str(item.get("msg_type"))
        message_id = _as_str(item.get("message_id"))
        raw_body = item.get("body")
        body = raw_body if isinstance(raw_body, dict) else {}
        raw_content = _as_str(body.get("content"))
        parsed = _parse_json_object(raw_content) if raw_content else None
        if parsed is None:
            continue
        if msg_type == "folder":
            key, name = _as_str(parsed.get("file_key")), _as_str(parsed.get("file_name"))
            if message_id and key:
                refs.append((message_id, message_id, key, name))
        elif msg_type == "post":
            for raw in _as_list(parsed.get("files")):
                f = raw if isinstance(raw, dict) else {}
                key, name = _as_str(f.get("file_key")), _as_str(f.get("file_name"))
                if _as_bool(f.get("is_folder")) and message_id and key:
                    refs.append((message_id + "\x00" + key, message_id, key, name))
    if not refs:
        return None
    sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

    async def one(ref: tuple[str, str, str, str]) -> tuple[str, str]:
        async with sem:
            return ref[0], await _fetch_folder_children_tree(get, ref[2], ref[3], ref[1])

    return dict(await asyncio.gather(*(one(ref) for ref in refs)))


# -- content converters --------------------------------------------------------


@dataclass
class _Ctx:
    """Everything a converter needs; mirrors Go's ConvertContext minus the Go runtime."""

    raw_content: str
    mention_map: dict[str, str] = field(default_factory=dict)
    mentions: list = field(default_factory=list)
    message_id: str = ""
    get: Get | None = None
    name_cache: dict[str, str] | None = None
    merge_prefetch: dict[str, list[dict]] | None = None
    folder_prefetch: dict[str, str] | None = None


async def _convert_text(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("text")
    text = _as_str(parsed.get("text"))
    if text == "":
        return ctx.raw_content  # not a text payload after all: show what the API sent
    return _resolve_mention_keys(text, ctx.mention_map)


async def _convert_image(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("image")
    key = _as_str(parsed.get("image_key"))
    return f"[Image: {key}]" if key else "[Image]"


async def _convert_file(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("file")
    key = _as_str(parsed.get("file_key"))
    if not key:
        return "[File]"
    name = _as_str(parsed.get("file_name")) or key
    return f'<file key="{_card_escape_attr(key)}" name="{_card_escape_attr(name)}"/>'


async def _convert_audio(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("audio")
    key = _as_str(parsed.get("file_key"))
    dur = _as_number(parsed.get("duration"))
    if key:
        out = f'<audio key="{_card_escape_attr(key)}"'
        if dur is not None and dur > 0:
            out += f' duration="{dur / 1000:.0f}s"'
        return out + "/>"
    if dur is not None and dur > 0:
        return f"[Voice: {dur / 1000:.0f}s]"
    return "[Voice]"


async def _convert_video(ctx: _Ctx) -> str:
    """Both "video" and "media" messages carry file_key plus optional duration and cover."""
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("video")
    key = _as_str(parsed.get("file_key"))
    if not key:
        return "[Video]"
    name = _as_str(parsed.get("file_name")) or key
    out = f'<video key="{_card_escape_attr(key)}" name="{_card_escape_attr(name)}"'
    dur = _as_number(parsed.get("duration"))
    if dur is not None and dur > 0:
        out += f' duration="{dur / 1000:.0f}s"'
    cover = _as_str(parsed.get("image_key"))
    if cover:
        out += f' cover_image_key="{_card_escape_attr(cover)}"'
    return out + "/>"


async def _convert_sticker(_ctx: _Ctx) -> str:
    return "[Sticker]"


async def _convert_video_chat(_ctx: _Ctx) -> str:
    return "[Video call]"


async def _convert_share_chat(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("chat card")
    chat_id = _as_str(parsed.get("chat_id"))
    return f"[Chat card: {chat_id}]" if chat_id else "[Chat card]"


async def _convert_share_user(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("user card")
    user_id = _as_str(parsed.get("user_id"))
    return f"[User card: {user_id}]" if user_id else "[User card]"


async def _convert_location(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("location")
    name = _as_str(parsed.get("name"))
    return f"[Location: {name}]" if name else "[Location]"


async def _convert_interactive(ctx: _Ctx) -> str:
    """Interactive cards live in .cards (ported separately); the raw card JSON goes in whole."""
    from .cards import render_card  # lazy: this module must import before cards.py lands

    return render_card(ctx.raw_content, mentions=ctx.mentions or None, names=ctx.name_cache)


async def _convert_hongbao(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("hongbao")
    text = _as_str(parsed.get("text"))
    return f"<hongbao text={_go_quote(text)}/>" if text else "<hongbao/>"


async def _convert_system(ctx: _Ctx) -> str:
    """System messages are a template with {placeholders} filled from sibling fields.

    Known placeholders ({from_user}, {to_chatters}, {divider_text}) are replaced even when
    their field is absent (with ""); any other {key} fills from a same-named string field
    and is left intact when there is nothing to fill it with, so the template stays legible.
    """
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("system message")
    template = _as_str(parsed.get("template"))
    if not template:
        return "[System message]"
    content = template
    for key in ("from_user", "to_chatters"):
        values = parsed.get(key)
        names = [v for v in values if isinstance(v, str) and v] if isinstance(values, list) else []
        content = content.replace("{" + key + "}", ", ".join(names))
    divider = parsed.get("divider_text")
    content = content.replace("{divider_text}", _as_str(divider.get("text")) if isinstance(divider, dict) else "")

    def fill(match: re.Match[str]) -> str:
        value = parsed.get(match.group(1))
        return value if isinstance(value, str) and value else match.group()

    return _SYSTEM_PLACEHOLDER.sub(fill, content).strip()


async def _convert_vote(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("vote")
    inner = []
    topic = _as_str(parsed.get("topic"))
    if topic:
        inner.append(topic)
    for option in _as_list(parsed.get("options")):
        if isinstance(option, str) and option:
            inner.append("• " + option)
    status = _as_number(parsed.get("status"))
    if status is not None and status != 0:
        inner.append("(Closed)")
    return "<vote>\n" + _xml_escape_body("\n".join(inner) if inner else "vote") + "\n</vote>"


async def _convert_todo(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("todo")
    task_id = _as_str(parsed.get("task_id"))
    task_attr = f' task_id="{_card_escape_attr(task_id)}"' if task_id else ""
    inner = []
    summary = parsed.get("summary")
    if isinstance(summary, dict):
        title = _as_str(summary.get("title"))
        if title:
            inner.append(title)
        text = _extract_post_blocks_text(_as_list(summary.get("content")))
        if text:
            inner.append(text)
    due = _as_str(parsed.get("due_time"))
    if due:
        formatted = _format_timestamp(due)
        if formatted:
            inner.append("Due: " + formatted)
    body = "\n".join(inner) if inner else "todo"
    return f"<todo{task_attr}>\n{_xml_escape_body(body)}\n</todo>"


def _convert_calendar(tag: str) -> Callable[[_Ctx], Awaitable[str]]:
    """Calendar messages render as a titled XML block; the tag names the card flavor."""

    async def convert(ctx: _Ctx) -> str:
        parsed = _parse_json_object(ctx.raw_content)
        if parsed is None:
            return _invalid("calendar")
        inner = []
        summary = _as_str(parsed.get("summary"))
        if summary:
            inner.append(summary)
        start = _format_timestamp(_as_str(parsed.get("start_time")))
        end = _format_timestamp(_as_str(parsed.get("end_time")))
        if start and end:
            inner.append(start + " ~ " + end)
        elif start:
            inner.append(start)
        attrs = ""
        for key, attr in (("open_calendar_id", "open_calendar_id"), ("open_event_id", "open_event_id"), ("share_token", "share_token")):
            value = _as_str(parsed.get(key))
            if value:
                attrs += f' {attr}="{_card_escape_attr(value)}"'
        body = "\n".join(inner) if inner else tag
        return f"<{tag}{attrs}>\n{_xml_escape_body(body)}\n</{tag}>"

    return convert


async def _render_post_attachments(ctx: _Ctx, parsed: dict) -> str:
    """The post attachment zone (top-level "files") renders as trailing <file>/<folder> tags.

    Folder attachments expand one level when possible, sharing the <folder> tag style with
    standalone folder messages so keys stay extractable everywhere.
    """
    lines = []
    for raw in _as_list(parsed.get("files")):
        f = raw if isinstance(raw, dict) else None
        if not f:
            continue
        key = _as_str(f.get("file_key"))
        if not key:
            continue
        name = _as_str(f.get("file_name"))
        tag = "file"
        if _as_bool(f.get("is_folder")):
            tag = "folder"
            expanded = ""
            cached = (ctx.folder_prefetch or {}).get(ctx.message_id + "\x00" + key)
            if cached is not None:
                expanded = cached  # "" = prefetch already failed: degrade, never retry inline
            elif ctx.get is not None and ctx.message_id:
                expanded = await _fetch_folder_children_tree(ctx.get, key, name, ctx.message_id)
            if expanded:
                lines.append(expanded)
                continue
        if name:
            lines.append(f'<{tag} key="{_card_escape_attr(key)}" name="{_card_escape_attr(name)}"/>')
        else:
            lines.append(f'<{tag} key="{_card_escape_attr(key)}"/>')
    return "\n".join(lines)


async def _convert_post(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("rich text")
    body = _unwrap_post_locale(parsed)
    parts = []
    if body is not None:
        title = _as_str(body.get("title"))
        if title:
            parts.append(title)
        for para in _select_content_blocks(body):
            parts.append("".join(_render_post_elem(el) for el in _as_list(para)))
    result = "\n".join(parts).strip() or "[Rich text message]"
    result = _resolve_mention_keys(result, ctx.mention_map)
    attachments = await _render_post_attachments(ctx, parsed)
    return result + "\n" + attachments if attachments else result


async def _convert_folder(ctx: _Ctx) -> str:
    parsed = _parse_json_object(ctx.raw_content)
    if parsed is None:
        return _invalid("folder")
    key = _as_str(parsed.get("file_key"))
    if not key:
        return "[Folder]"
    name = _as_str(parsed.get("file_name"))
    if ctx.folder_prefetch is not None and ctx.message_id:
        cached = ctx.folder_prefetch.get(ctx.message_id)
        if cached is not None:
            return cached or _fallback_folder_line(key, name)  # "" = prefetch failed: degrade
    if ctx.get is not None and ctx.message_id:
        tree = await _fetch_folder_children_tree(ctx.get, key, name, ctx.message_id)
        if tree:
            return tree
    return _fallback_folder_line(key, name)


# -- merge_forward expansion (merge.go) -----------------------------------------


def _parse_merge_forward_ids(raw: str) -> list[str]:
    parsed = _parse_json_object(raw)
    if parsed is None:
        return []
    return [i for i in _as_list(parsed.get("create_message_ids")) if isinstance(i, str)]


def _merge_item_timestamp(item: dict) -> int:
    n = _scan_leading_int(_as_str(item.get("create_time")))
    return n or 0


def _build_merge_children(items: list[dict], root_id: str) -> dict[str, list[dict]]:
    """Parent -> children from a flat sub-message list; root's own row is skipped.

    Items without upper_message_id are direct children of the merge_forward container,
    and each group is ordered by create_time so the tree reads chronologically.
    """
    children: dict[str, list[dict]] = {}
    for item in items:
        message_id = _as_str(item.get("message_id"))
        upper_id = _as_str(item.get("upper_message_id"))
        if message_id == root_id and not upper_id:
            continue
        children.setdefault(upper_id or root_id, []).append(item)
    for group in children.values():
        group.sort(key=_merge_item_timestamp)
    return children


def _indent_lines(text: str, indent: str) -> str:
    return "\n".join(indent + line for line in text.split("\n"))


async def _format_merge_sub_tree(ctx: _Ctx, parent_id: str, children: dict[str, list[dict]]) -> str:
    """Render the forwarded tree: [timestamp] sender: plus the indented message body.

    Nested merge_forwards recurse through the tree (no API calls); other types convert
    with the plain converter and no fetch context, exactly like lark-cli's sub-tree path.
    """
    group = children.get(parent_id) or []
    if not group:
        return "<forwarded_messages/>"
    parts = []
    for item in group:
        msg_type = _as_str(item.get("msg_type")) or "text"
        raw_sender = item.get("sender")
        sender = raw_sender if isinstance(raw_sender, dict) else {}
        name, sid = _as_str(sender.get("name")), _as_str(sender.get("id"))
        sender_label = name or sid or "unknown"
        timestamp = _format_merge_timestamp(_as_str(item.get("create_time")))
        message_id = _as_str(item.get("message_id"))
        if msg_type == "merge_forward" and message_id:
            content = await _format_merge_sub_tree(ctx, message_id, children)
        else:
            raw_body = item.get("body")
            body = raw_body if isinstance(raw_body, dict) else {}
            mentions = _as_list(item.get("mentions"))
            content = await _convert_body_content(
                msg_type,
                _Ctx(_as_str(body.get("content")), _build_mention_key_map(mentions), mentions),
            )
        parts.append(f"[{timestamp}] {sender_label}:\n{_indent_lines(content, '    ')}")
    return "<forwarded_messages>\n" + "\n".join(parts) + "\n</forwarded_messages>"


async def _render_merge_tree(ctx: _Ctx, sub_items: list[dict]) -> str:
    """Resolve sub-item senders and render the tree; shared by prefetch and inline fetch."""
    items = [dict(item) for item in sub_items if isinstance(item, dict)]
    for item in items:
        item["sender"] = _copy_sender(item.get("sender"))
    _resolve_sender_names(items, ctx.name_cache)
    _attach_sender_names(items, ctx.name_cache or {})
    return await _format_merge_sub_tree(ctx, ctx.message_id, _build_merge_children(items, ctx.message_id))


async def _fetch_merge_sub_messages(get: Get, message_id: str) -> list[dict]:
    """GET /messages/{id} lists a merge_forward container's sub-messages flat, oldest first."""
    data = await get(
        "GET",
        f"/open-apis/im/v1/messages/{message_id}",
        params={"user_id_type": "open_id", "card_msg_content_type": "raw_card_content", "with_sender_name": "true"},
    )
    return [m for m in _as_list(data.get("items")) if isinstance(m, dict)] if isinstance(data, dict) else []


async def _convert_merge_forward(ctx: _Ctx) -> str:
    """A merge_forward body is a placeholder string; the tree only exists in the API.

    The prefetch cache decides everything: a hit renders the tree (even an empty one, for
    a recalled container), and a miss falls back to one inline GET or to the id list
    embedded in older content formats.
    """
    if ctx.merge_prefetch is not None and ctx.message_id and ctx.message_id in ctx.merge_prefetch:
        return await _render_merge_tree(ctx, ctx.merge_prefetch[ctx.message_id])
    if ctx.get is not None and ctx.message_id:
        try:
            sub_items = await _fetch_merge_sub_messages(ctx.get, ctx.message_id)
        except Exception as exc:  # noqa: BLE001 - a failed fetch is rendered, not raised
            return f"[Merged forward: fetch failed: {exc}]"
        if sub_items:
            return await _render_merge_tree(ctx, sub_items)
    ids = _parse_merge_forward_ids(ctx.raw_content)
    return f"[Merged forward: {len(ids)} messages]" if ids else "[Merged forward]"


async def _prefetch_merge_sub_items(items: list[dict], get: Get | None, name_cache: dict[str, str]) -> dict[str, list[dict]] | None:
    """merge.go PrefetchMergeForwardSubItems: fetch every container's sub-messages once, up front.

    Fetch failures leave the id out of the map so the converter retries inline and can
    surface "[Merged forward: fetch failed: ...]" instead of silently rendering an empty
    tree. One batched sender-name harvest keeps the render loop free of name lookups.
    """
    if get is None or not items:
        return None
    ids = []
    for item in items:
        # "merge_message" is Feishu's recalled-merge marker; it renders through the same
        # converter and prefetch as merge_forward (fixtures confirm: "<forwarded_messages/>").
        if _as_str(item.get("msg_type")) in ("merge_forward", "merge_message"):
            message_id = _as_str(item.get("message_id"))
            if message_id:
                ids.append(message_id)
    if not ids:
        return None
    sem = asyncio.Semaphore(_FETCH_CONCURRENCY)
    result: dict[str, list[dict]] = {}

    async def one(message_id: str) -> None:
        async with sem:
            try:
                result[message_id] = await _fetch_merge_sub_messages(get, message_id)
            except Exception as exc:  # noqa: BLE001 - failure stays out of the cache, see above
                print(f"warning: merge_forward_prefetch_failed: {message_id}: {exc}", file=sys.stderr)

    await asyncio.gather(*(one(message_id) for message_id in ids))
    all_sub_items = [item for group in result.values() for item in group]
    if all_sub_items:
        _resolve_sender_names(all_sub_items, name_cache)
    return result


# -- body content dispatch (content_convert.go) ---------------------------------

_CONVERTERS: dict[str, Callable[[_Ctx], Awaitable[str]]] = {
    "text": _convert_text,
    "post": _convert_post,
    "image": _convert_image,
    "file": _convert_file,
    "audio": _convert_audio,
    "video": _convert_video,
    "media": _convert_video,
    "sticker": _convert_sticker,
    "interactive": _convert_interactive,
    "share_chat": _convert_share_chat,
    "share_user": _convert_share_user,
    "location": _convert_location,
    "merge_forward": _convert_merge_forward,
    "merge_message": _convert_merge_forward,
    "folder": _convert_folder,
    "share_calendar_event": _convert_calendar("calendar_share"),
    "calendar": _convert_calendar("calendar_invite"),
    "general_calendar": _convert_calendar("calendar"),
    "video_chat": _convert_video_chat,
    "system": _convert_system,
    "todo": _convert_todo,
    "vote": _convert_vote,
    "hongbao": _convert_hongbao,
}


async def _convert_body_content(msg_type: str, ctx: _Ctx) -> str:
    """ConvertBodyContent: per-type render, or "[msg_type]" for anything unregistered."""
    if ctx.raw_content == "":
        return ""
    converter = _CONVERTERS.get(msg_type)
    if converter is None:
        return f"[{msg_type}]"
    return await converter(ctx)


# -- view assembly (content_convert.go formatMessageItem) -----------------------


def _simplified_mentions(mentions: list) -> list[dict]:
    out = []
    for raw in mentions:
        item = raw if isinstance(raw, dict) else {}
        out.append({"key": _as_str(item.get("key")), "id": _extract_mention_open_id(item.get("id")), "name": _as_str(item.get("name"))})
    return out


async def _format_message_item(
    raw: dict,
    *,
    get: Get | None,
    name_cache: dict[str, str] | None,
    merge_prefetch: dict[str, list[dict]] | None,
    folder_prefetch: dict[str, str] | None,
) -> dict:
    """One raw API item -> one view dict, mirroring Go's formatMessageItem key for key.

    update_time is only carried when `updated` is set: the server echoes
    update_time == create_time for unedited messages, and surfacing that would make every
    message look edited downstream.
    """
    msg_type = _as_str(raw.get("msg_type"))
    message_id = _as_str(raw.get("message_id"))
    mentions = _as_list(raw.get("mentions"))
    raw_body = raw.get("body")
    body = raw_body if isinstance(raw_body, dict) else {}
    raw_content = _as_str(body.get("content"))
    content = await _convert_body_content(
        msg_type,
        _Ctx(
            raw_content,
            _build_mention_key_map(mentions),
            mentions,
            message_id,
            get,
            name_cache,
            merge_prefetch,
            folder_prefetch,
        ),
    )

    view: dict[str, Any] = {
        "message_id": message_id,
        "msg_type": msg_type,
        "content": content,
        "sender": _copy_sender(raw.get("sender")),
        "create_time": _format_time(raw.get("create_time")),
        "deleted": _as_bool(raw.get("deleted")),
        "updated": _as_bool(raw.get("updated")),
    }

    thread_id, parent_id = _as_str(raw.get("thread_id")), _as_str(raw.get("parent_id"))
    if thread_id:
        view["thread_id"] = thread_id  # thread_id wins; reply_to is for replies outside a thread
    elif parent_id:
        view["reply_to"] = parent_id

    if view["updated"] and "update_time" in raw and raw["update_time"] is not None:
        value = raw["update_time"]
        if isinstance(value, str):
            if value.strip():
                view["update_time"] = _format_time(value)
        else:
            view["update_time"] = _format_time(value)
    for key in ("chat_id", "message_position", "thread_message_position"):
        if key in raw:
            view[key] = raw[key]

    app_link = _as_str(raw.get("message_app_link")).strip() or _assemble_message_app_link(raw)
    if app_link:
        view["message_app_link"] = app_link

    if mentions:
        view["mentions"] = _simplified_mentions(mentions)
    return view


# -- thread inlining (thread.go) ------------------------------------------------


@dataclass
class _ThreadResult:
    replies: list[dict] | None  # None = fetch failed (Go's nil slice)
    has_more: bool


async def _fetch_thread_replies(get: Get, thread_id: str, limit: int) -> _ThreadResult:
    data = await get(
        "GET",
        "/open-apis/im/v1/messages",
        params={
            "container_id_type": "thread",
            "container_id": thread_id,
            "sort_type": "ByCreateTimeAsc",
            "page_size": str(limit),
            "card_msg_content_type": "raw_card_content",
            "with_sender_name": "true",
        },
    )
    items = [m for m in _as_list(data.get("items")) if isinstance(m, dict)] if isinstance(data, dict) else []
    return _ThreadResult(items, bool(data.get("has_more")) if isinstance(data, dict) else False)


async def _expand_thread_replies(views: list[dict], get: Get | None, name_cache: dict[str, str]) -> None:
    """Embed up to 50 replies under the first view referencing each thread.

    The 500-reply total budget is spent on actual returned counts (not the planned 50 per
    thread) in first-seen order, so a chat with many short threads still inlines all of
    them; a thread cut by the budget keeps thread_has_more so the rest stay discoverable.
    """
    if get is None:
        return
    plans: list[tuple[str, dict]] = []
    seen: set[str] = set()
    for view in views:
        thread_id = _as_str(view.get("thread_id"))
        if thread_id and thread_id not in seen:
            seen.add(thread_id)
            plans.append((thread_id, view))
    if not plans:
        return

    sem = asyncio.Semaphore(_FETCH_CONCURRENCY)

    async def one(thread_id: str) -> _ThreadResult:
        async with sem:
            try:
                return await _fetch_thread_replies(get, thread_id, _THREAD_REPLIES_PER_THREAD)
            except Exception:  # noqa: BLE001 - fetch failure is flagged on the host, never raised
                return _ThreadResult(None, False)

    results = list(await asyncio.gather(*(one(thread_id) for thread_id, _ in plans)))

    remaining = _THREAD_REPLIES_TOTAL_LIMIT
    for result in results:
        if result.replies is None or not result.replies:
            continue
        if remaining <= 0:
            result.replies = []  # budget drained by earlier threads: discard, no error flag
            continue
        if len(result.replies) > remaining:
            result.replies = result.replies[:remaining]
            result.has_more = True
        remaining -= len(result.replies)

    all_raw_replies = [reply for result in results if result.replies for reply in result.replies]
    merge_prefetch = await _prefetch_merge_sub_items(all_raw_replies, get, name_cache)
    folder_prefetch = await _prefetch_folder_children(all_raw_replies, get)

    prepared: list[list[dict] | None] = []
    for (_, host), result in zip(plans, results, strict=True):
        if result.replies is None:
            host["thread_replies_error"] = True
            prepared.append(None)
            continue
        if not result.replies:
            prepared.append(None)
            continue
        replies = [await _format_message_item(raw, get=get, name_cache=name_cache, merge_prefetch=merge_prefetch, folder_prefetch=folder_prefetch) for raw in result.replies]
        _resolve_sender_names(replies, name_cache)
        prepared.append(replies)

    for (_, host), result, replies in zip(plans, results, prepared, strict=True):
        if replies is None:
            continue
        _attach_sender_names(replies, name_cache)
        host["thread_replies"] = replies
        if result.has_more:
            host["thread_has_more"] = True


# -- reactions enrichment (reactions.go) ----------------------------------------


def _collect_message_nodes(views: list[dict]) -> tuple[dict[str, list[dict]], list[str]]:
    """Index every view (including thread replies) by message_id; ids dedupe first-seen."""
    index: dict[str, list[dict]] = {}
    ids: list[str] = []

    def walk(messages: list[dict]) -> None:
        for msg in messages:
            message_id = _as_str(msg.get("message_id"))
            if message_id:
                if message_id not in index:
                    ids.append(message_id)
                index.setdefault(message_id, []).append(msg)
            nested = msg.get("thread_replies")
            if isinstance(nested, list):
                walk([m for m in nested if isinstance(m, dict)])

    walk(views)
    return index, ids


def _group_reactions(raw: Any, key: str) -> dict[str, list]:
    groups: dict[str, list] = {}
    for item in _as_list(raw):
        row = item if isinstance(item, dict) else {}
        message_id = _as_str(row.get("message_id"))
        entries = _as_list(row.get(key))
        if message_id and entries:
            groups.setdefault(message_id, []).extend(entries)
    return groups


async def _enrich_reactions(views: list[dict], get: Get | None) -> None:
    """Attach one "reactions" block per message the server has data for, in batches of 20.

    Enrichment must never break message output: a failed batch flags its messages with
    reactions_error (like thread_replies_error) and moves on, and per-message failures in
    an otherwise good response flag only those messages.
    """
    if get is None:
        return
    index, ids = _collect_message_nodes(views)
    if not ids:
        return
    for start in range(0, len(ids), _REACTIONS_BATCH_MAX):
        batch = ids[start : start + _REACTIONS_BATCH_MAX]
        try:
            data = await get("POST", "/open-apis/im/v1/messages/reactions/batch_query", body={"queries": [{"message_id": message_id} for message_id in batch]})
        except Exception as exc:  # noqa: BLE001 - see docstring
            print(f"warning: reactions_batch_query_failed: {exc}", file=sys.stderr)
            for message_id in batch:
                for msg in index.get(message_id, []):
                    msg["reactions_error"] = True
            continue
        counts = _group_reactions(data.get("success_msg_reaction_counts") if isinstance(data, dict) else None, "reaction_count")
        details = _group_reactions(data.get("success_msg_reaction_details") if isinstance(data, dict) else None, "message_reaction_items")
        for message_id in batch:
            block: dict[str, Any] = {}
            if counts.get(message_id):
                block["counts"] = counts[message_id]
            if details.get(message_id):
                block["details"] = details[message_id]
            if not block:
                continue
            for msg in index.get(message_id, []):
                msg["reactions"] = block
        failed = [_as_str(item.get("message_id")) for item in _as_list(data.get("fail_msg_reaction_details") if isinstance(data, dict) else None) if isinstance(item, dict)]
        for message_id in failed:
            if message_id:
                for msg in index.get(message_id, []):
                    msg["reactions_error"] = True


# -- the on-disk form (sync.py _clean) ------------------------------------------

_RE_ENTITY = re.compile(r"&(#\d+|#[xX][0-9a-fA-F]+|\w+);")


def _clean_text(text: str) -> str:
    """lark-fs writes _clean(msg) to disk, so the on-disk view is entity-unescaped.

    Search results arrive HTML-escaped with <h> hit markers and message rendering escapes
    markup (vote/todo/calendar bodies); unescaping the semicolon form restores the plain
    text `rg` should see. Only the semicolon form is decoded -- html.unescape alone would
    also eat "&timestamp=" out of URLs.
    """
    return _RE_ENTITY.sub(lambda m: html.unescape(m.group()), text.replace("<h>", "").replace("</h>", ""))


def _clean_value(value: Any) -> Any:
    """The same pass over every string in a view, exactly as sync._clean applies it."""
    if isinstance(value, str):
        return _clean_text(value)
    if isinstance(value, dict):
        return {key: _clean_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_clean_value(item) for item in value]
    return value


# -- entry points ---------------------------------------------------------------


async def message_views(raw_items: list[dict], *, get: Get, reactions: bool = True) -> list[dict]:
    """lark-cli's message view for a page of raw API message items (the `items` of
    GET /open-apis/im/v1/messages or GET /open-apis/im/v1/messages/mget, fetched with
    with_sender_name=true and card_msg_content_type=raw_card_content):
    - `content` rendered per msg_type (port of ConvertBodyContent)
    - create_time formatted exactly as lark-cli formats it (verify against fixtures!)
    - sender normalized exactly as lark-cli does (sender_name -> name etc.)
    - thread_id / reply_to per the Go logic, update_time only when updated, chat_id,
      message_position, thread_message_position, mentions, message_app_link
    - thread roots: inline up to 50 replies as `thread_replies` and set `thread_has_more`
      exactly like lark-cli (thread.go; fetch the thread container through `get` when needed)
    - merge_forward: expand sub-messages like lark-cli (merge.go; fetch through `get`)
    - reactions enrichment (unless reactions=False): one
      POST /open-apis/im/v1/messages/reactions/batch_query per 20 messages, aggregated into
      the view's `reactions` field exactly as lark-cli does (reactions.go)
    Unknown/unsupported msg_types render like lark-cli does ('[nonsupport]', '[unknown]', ...).
    """
    items = [item for item in raw_items if isinstance(item, dict)]
    name_cache: dict[str, str] = {}
    merge_prefetch = await _prefetch_merge_sub_items(items, get, name_cache)
    folder_prefetch = await _prefetch_folder_children(items, get)
    views = [await _format_message_item(item, get=get, name_cache=name_cache, merge_prefetch=merge_prefetch, folder_prefetch=folder_prefetch) for item in items]
    _resolve_sender_names(views, name_cache)
    _attach_sender_names(views, name_cache)
    await _expand_thread_replies(views, get, name_cache)
    if reactions:
        await _enrich_reactions(views, get)
    # The on-disk form lark-fs compares against is _clean()'d; doing it here keeps
    # message_view() == the dict that reaches disk. Callers must not unescape again.
    return [_clean_value(view) for view in views]


async def message_view(raw: dict, *, get: Get, reactions: bool = True) -> dict:
    """One raw API item -> one view; exactly message_views([raw])[0]."""
    return (await message_views([raw], get=get, reactions=reactions))[0]
