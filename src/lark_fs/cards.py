"""Rendering an interactive message's body.content exactly as lark-cli's message view does.

The mirror is diffed against files lark-cli wrote earlier, so byte-identical output is the
contract: odd tokens like '<card>', '📝' and '[Unknown content]' are lark-cli's own
conventions and stay. This is card.go's concise renderer -- its "detailed" mode is never
constructed there, so the id-suffixed variants are omitted.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from json import loads
from math import floor, isfinite
from typing import Any

UNPARSED = "[interactive card]"
UNABLE_TO_PARSE = "<card>\n[Unable to parse card content]\n</card>"

_EMOJI = {"OK": "👌", "THUMBSUP": "👍", "SMILE": "😊", "HEART": "❤️", "CLAP": "👏", "FIRE": "🔥", "PARTY": "🎉", "THINK": "🤔"}

_CHART_TYPES = {"bar": "Bar chart", "line": "Line chart", "pie": "Pie chart", "area": "Area chart", "radar": "Radar chart", "scatter": "Scatter plot"}

_INT_RE = re.compile(r"^[+-]?[0-9]+$")
_GO_MAP_KEY_RE = re.compile(r" [a-zA-Z_][a-zA-Z0-9_]*:")


# ── JSON shape guards ─────────────────────────────────────────────────────────
# Go type-asserts every field; a wrong type reads as absent, never as an error.


def _f64(raw: str) -> float:
    """Parse a JSON number like encoding/json: everything becomes float64 or fails."""
    value = float(raw)
    if not isfinite(value):
        raise ValueError(f"number out of range: {raw}")  # noqa: TRY003
    return value


def _parse(raw: str) -> dict[str, Any] | None:
    """json.Unmarshal into a map; only a JSON object (or null) is acceptable."""
    try:
        value = loads(raw, parse_int=_f64, parse_float=_f64)
    except ValueError:
        return None
    if value is None:
        return {}
    return value if isinstance(value, dict) else None


def _obj(value: Any) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _list(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def _string(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _number(value: Any) -> float | None:
    return value if isinstance(value, float) else None


def _boolean(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


# ── Number and string formatting ──────────────────────────────────────────────


def _fmt_f(value: float) -> str:
    """strconv.FormatFloat(v, 'f', -1, 64): shortest decimal, never an exponent."""
    text = format(Decimal(repr(value)), "f")
    return text.removesuffix(".0")


def _fmt_g(value: float) -> str:
    """Go %v for float64 (%g shortest): exponent form past the significant digits."""
    dec = Decimal(repr(value))
    digits = len(dec.as_tuple().digits)
    if dec and (dec.adjusted() < -4 or dec.adjusted() >= digits):
        return repr(value)
    return _fmt_f(value)


def _fmt_v(value: Any) -> str:
    """Go %v for chart datum lookups, where a missing key prints as <nil>."""
    if value is None:
        return "<nil>"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return _fmt_g(value)
    return value if isinstance(value, str) else str(value)


def _scrub(text: str) -> str:
    """Drop bare control characters, keeping only the newline and tab the text uses.

    lark-cli's recorded message views contain no C0 controls at all: a card whose text
    carries ANSI codes is recorded with the ESC byte dropped and the readable tail
    ('[36m') intact, so reproducing the mirror means dropping them after rendering.
    """
    return "".join(c for c in text if c in "\n\t" or 0x20 <= ord(c) < 0x7F or ord(c) > 0x7F)


def _escape_attr(text: str) -> str:
    return text.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def _escape_link_text(text: str) -> str:
    return text.replace("[", "\\[").replace("]", "\\]")


def _go_map_texts(raw: str) -> list[str]:
    """Extract 'text' values out of a Go fmt map-array literal like "[map[text:VIP] map[text:Premium]]"."""
    if not raw.startswith("[") or "map[" not in raw:
        return []
    texts: list[str] = []
    rest = raw
    while True:
        index = rest.find("text:")
        if index < 0:
            return texts
        after = rest[index + len("text:") :]
        bracket_end = after.find("]")
        next_key = _GO_MAP_KEY_RE.search(after)
        if next_key and (bracket_end < 0 or next_key.start() < bracket_end):
            end = next_key.start()
        elif bracket_end >= 0:
            end = bracket_end
        else:
            if after:
                texts.append(after)
            return texts
        if after[:end]:
            texts.append(after[:end])
        rest = after[end:]


def _int64(raw: str) -> int | None:
    """strconv.ParseInt(raw, 10, 64): strict digits with a range check."""
    if not _INT_RE.match(raw):
        return None
    value = int(raw)
    return value if -(2**63) <= value < 2**63 else None


def _rfc3339(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _millis_iso(raw: str) -> str:
    n = _int64(raw)
    if n is None:
        return ""
    return _rfc3339(n / 1000)


def _normalize_time(raw: str) -> str:
    """Date pickers carry bare epoch strings; lark-cli prints RFC3339 for those."""
    if not raw:
        return ""
    n = _int64(raw)
    if n is not None:
        if len(raw) >= 13:
            return _rfc3339(n / 1000)
        if len(raw) >= 10:
            return _rfc3339(n)
    return raw


# ── Text extraction ───────────────────────────────────────────────────────────


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    obj = _obj(value)
    if obj is None:
        return ""
    prop = _obj(obj.get("property"))
    return _text_property(obj if prop is None else prop)


def _text_property(prop: dict[str, Any]) -> str:
    i18n = _obj(prop.get("i18nContent"))
    if i18n is not None:
        for lang in ("zh_cn", "en_us", "ja_jp"):
            translated = _string(i18n.get(lang))
            if translated:
                return translated
    content = _string(prop.get("content"))
    if content is not None:
        return content
    elements = _list(prop.get("elements"))
    if elements:
        return "".join(t for t in (_text(el) for el in elements) if t != "")
    return _string(prop.get("text")) or ""


def _style(content: str, prop: dict[str, Any]) -> str:
    text_style = _obj(prop.get("textStyle"))
    attributes = {a for a in (_string(v) for v in _list(text_style.get("attributes")) or []) if a is not None} if text_style is not None else set()
    if "strikethrough" in attributes:
        content = f"~~{content}~~"
    if "italic" in attributes:
        content = f"*{content}*"
    if "bold" in attributes:
        content = f"**{content}**"
    return content


# ── Mentions ──────────────────────────────────────────────────────────────────


def _mention_open_id(value: Any) -> str:
    if isinstance(value, str):
        return value
    obj = _obj(value)
    return _string(obj.get("open_id")) or "" if obj is not None else ""


@dataclass(frozen=True)
class _Ctx:
    """What the card renderer carries down the tree: the parsed json_attachment maps,
    the message mentions by key, and the caller's open_id -> display name cache."""

    attachment: dict[str, Any] | None
    mentions_by_key: dict[str, dict[str, Any]]
    names: dict[str, str]


def _person_name(ctx: _Ctx, user_id: str) -> str:
    """`names` is the caller's cache and wins; lark-cli's own source is the attachment."""
    cached = ctx.names.get(user_id)
    if cached:
        return cached
    persons = _obj(ctx.attachment.get("persons")) if ctx.attachment is not None else None
    person = _obj(persons.get(user_id)) if persons is not None else None
    return (_string(person.get("content")) or "") if person is not None else ""


def _option_user_name(ctx: _Ctx, user_id: str) -> str:
    cached = ctx.names.get(user_id)
    if cached:
        return cached
    option_users = _obj(ctx.attachment.get("option_users")) if ctx.attachment is not None else None
    info = _obj(option_users.get(user_id)) if option_users is not None else None
    return (_string(info.get("content")) or "") if info is not None else ""


def _image_key(ctx: _Ctx, image_id: str) -> tuple[str, str]:
    images = _obj(ctx.attachment.get("images")) if ctx.attachment is not None else None
    info = _obj(images.get(image_id)) if images is not None else None
    if info is None:
        return "", ""
    return _string(info.get("origin_key")) or "", _string(info.get("token")) or ""


# ── Element dispatch ──────────────────────────────────────────────────────────


def _elements(ctx: _Ctx, elements: list[Any], depth: int) -> str:
    results = [r for r in (_element(ctx, _obj(el) or {}, depth) for el in elements) if r]
    return "\n".join(results)


def _md_elements(ctx: _Ctx, elements: list[Any]) -> str:
    return "".join(r for r in (_element(ctx, _obj(el) or {}, 0) for el in elements) if r)


def _element(ctx: _Ctx, elem: dict[str, Any], depth: int) -> str:
    tag = _string(elem.get("tag")) or ""
    elem_id = _string(elem.get("id")) or ""
    prop = _obj(elem.get("property"))
    handler = _ELEMENTS.get(tag)
    if handler is None:
        return _unknown(ctx, elem if prop is None else prop, tag)
    return handler(ctx, elem, elem if prop is None else prop, elem_id, depth)


def _plain_text(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    content = _string(prop.get("content")) or ""
    return _style(content, prop) if content else ""


def _markdown(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    elements = _list(prop.get("elements"))
    if elements:
        return _md_elements(_ctx, elements)
    return _string(prop.get("content")) or ""


def _markdown_v1(_ctx: _Ctx, elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    elements = _list(prop.get("elements"))
    if elements:
        return _md_elements(_ctx, elements)
    fallback = _obj(elem.get("fallback"))
    if fallback is not None:
        return _element(_ctx, fallback, 0)
    return _string(prop.get("content")) or ""


def _div(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    results: list[str] = []
    text_elem = _obj(prop.get("text"))
    if text_elem is not None:
        text = _element(ctx, text_elem, 0)
        if text:
            text_prop = _obj(text_elem.get("property")) or text_elem
            text_style = _obj(text_prop.get("textStyle"))
            if text_style is not None and _string(text_style.get("size")) == "notation":
                text = "📝 " + text
            results.append(text)
    fields = _list(prop.get("fields"))
    if fields:
        field_texts = []
        for field in fields:
            fm = _obj(field)
            te = _obj(fm.get("text")) if fm is not None else None
            if te is not None:
                ft = _element(ctx, te, 0)
                if ft:
                    field_texts.append(ft)
        if field_texts:
            results.append("\n".join(field_texts))
    extra_elem = _obj(prop.get("extra"))
    if extra_elem is not None:
        extra = _element(ctx, extra_elem, 0)
        if extra:
            results.append(extra)
    return "\n".join(results)


def _note(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    elements = _list(prop.get("elements")) or []
    texts = [t for t in (_element(ctx, _obj(el) or {}, 0) for el in elements) if t]
    return "📝 " + " ".join(texts) if texts else ""


def _constant(text: str) -> Callable[[_Ctx, dict[str, Any], dict[str, Any], str, int], str]:
    return lambda _ctx, _elem, _prop, _id, _depth: text


def _column_set(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, depth: int) -> str:
    columns = _list(prop.get("columns")) or []
    results = [r for r in (_element(ctx, _obj(col) or {}, depth + 1) for col in columns) if r]
    return (" " if _all_buttons(results) else "\n\n").join(results)


def _all_buttons(results: list[str]) -> bool:
    return bool(results) and all(r.startswith("[") and "\n" not in r for r in results)


def _column(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, depth: int) -> str:
    elements = _list(prop.get("elements"))
    return _elements(ctx, elements, depth) if elements else ""


def _person(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    user_id = _string(prop.get("userID")) or ""
    if not user_id:
        return ""
    name = _person_name(ctx, user_id)
    if not name:
        notation = _obj(prop.get("notation"))
        name = _text(notation) if notation is not None else ""
    return name or user_id


def _person_v1(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    user_id = _string(prop.get("userID")) or ""
    return _person_name(ctx, user_id) or user_id if user_id else ""


def _person_list(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    persons = _list(prop.get("persons")) or []
    names: list[str] = []
    for person in persons:
        pm = _obj(person)
        person_id = _string(pm.get("id")) or "" if pm is not None else ""
        name = _person_name(ctx, person_id)
        names.append(name or person_id or "user")
    return ", ".join(names)


def _avatar(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    user_id = _string(prop.get("userID")) or ""
    name = _person_name(ctx, user_id)
    if name:
        return "👤 " + name
    return "👤" + (f"(id:{user_id})" if user_id else "")


def _at(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    user_id = _string(prop.get("userID")) or ""
    if not user_id:
        return ""
    user_name = ""
    actual_id = ""
    from_mentions = False
    at_users = _obj(ctx.attachment.get("at_users")) if ctx.attachment is not None else None
    info = _obj(at_users.get(user_id)) if at_users is not None else None
    if info is not None:
        user_name = _string(info.get("content")) or ""
        actual_id = _string(info.get("user_id")) or ""
        mention_key = _string(info.get("mention_key")) or ""
        if mention_key:
            mention = ctx.mentions_by_key.get(mention_key)
            if mention is not None:
                name = _string(mention.get("name")) or ""
                if name:
                    user_name = name
                open_id = _mention_open_id(mention.get("id"))
                if open_id:
                    actual_id = open_id
                    from_mentions = True
    if not user_name:
        user_name = ctx.names.get(user_id) or ""
    if user_name:
        return f"@{user_name}({actual_id if from_mentions and actual_id else user_id})"
    return "@" + user_id


def _button(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    text_elem = _obj(prop.get("text"))
    button_text = _text(text_elem) if text_elem is not None else ""
    button_text = button_text or "Button"
    if _boolean(prop.get("disabled")):
        result = f"[{button_text} ✗]"
        tips = _obj(prop.get("disabledTips"))
        tips_text = _text(tips) if tips is not None else ""
        return result + f'(tips:"{tips_text}")' if tips_text else result
    result = f"[{button_text}]"
    for action in _list(prop.get("actions")) or []:
        am = _obj(action)
        ad = _obj(am.get("action")) if am is not None and _string(am.get("type")) == "open_url" else None
        url = _string(ad.get("url")) if ad is not None else None
        if url:
            result = f"[{_escape_link_text(button_text)}]({url})"
            break
    confirm = _obj(prop.get("confirm"))
    if confirm is not None:
        parts = [_text(confirm[key]) for key in ("title", "text") if key in confirm]
        parts = [p for p in parts if p]
        if parts:
            result += f'(confirm:"{": ".join(parts)}")'
    return result


def _actions(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    actions = _list(prop.get("actions")) or []
    return " ".join(r for r in (_element(ctx, _obj(a) or {}, 0) for a in actions) if r)


def _overflow(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    options = _list(prop.get("options")) or []
    texts: list[str] = []
    for option in options:
        om = _obj(option)
        text_elem = _obj(om.get("text")) if om is not None else None
        text = _text(text_elem) if text_elem is not None else ""
        if not text:
            continue
        url = ""
        owner = om if isinstance(om, dict) else {}
        for action in _list(owner.get("actions")) or []:
            am = _obj(action)
            if am is not None and _string(am.get("type")) == "open_url":
                ad = _obj(am.get("action"))
                url = (_string(ad.get("url")) or "") if ad is not None else ""
        if url:
            text = f"[{_escape_link_text(text)}]({url})"
        elif _string(owner.get("value")) or "":
            text += f"({_string(owner.get('value'))})"
        texts.append(text)
    return "⋮ " + ", ".join(texts)


def _select(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int, multi: bool = False) -> str:
    options = _list(prop.get("options")) or []
    selected: set[str] = set()
    if multi:
        selected = {v for v in (_string(v) for v in _list(prop.get("selectedValues")) or []) if v is not None}
    else:
        initial = _string(prop.get("initialOption"))
        if initial is not None:
            selected.add(initial)
        index = _number(prop.get("initialIndex"))
        if index is not None:
            i = int(index)
            if 0 <= i < len(options):
                om = _obj(options[i])
                value = _string(om.get("value")) if om is not None else None
                if value is not None:
                    selected.add(value)
    option_texts: list[str] = []
    has_selected = False
    for option in options:
        om = _obj(option)
        if om is None:
            continue
        value = _string(om.get("value")) or ""
        text_elem = _obj(om.get("text"))
        opt_text = _text(text_elem) if text_elem is not None else ""
        opt_text = opt_text or _option_user_name(ctx, value) or value
        if not opt_text:
            continue
        if value in selected:
            opt_text = "✓" + opt_text
            has_selected = True
        option_texts.append(opt_text)
    if not option_texts:
        placeholder = "Please select"
        ph_elem = _obj(prop.get("placeholder"))
        ph = _text(ph_elem) if ph_elem is not None else ""
        option_texts.append((ph or placeholder) + " ▼")
    elif not has_selected:
        option_texts[-1] += " ▼"
    return "{" + " / ".join(option_texts) + "}" + ("(multi)" if multi else "")


def _select_img(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    options = _list(prop.get("options")) or []
    if not options:
        return ""
    selected = {v for v in (_string(v) for v in _list(prop.get("selectedValues")) or []) if v is not None}
    texts: list[str] = []
    for i, option in enumerate(options):
        om = _obj(option)
        if om is None:
            continue
        value = _string(om.get("value")) or ""
        text = f"🖼️ Image {i + 1}" + (f"({value})" if value else "")
        image_id = _string(om.get("imageID"))
        if image_id:
            origin_key, token = _image_key(_ctx, image_id)
            if origin_key:
                text += f"(img_key:{origin_key})"
            elif token:
                text += f"(img_token:{token})"
        texts.append(("✓" + text) if value in selected else text)
    return "{" + " / ".join(texts) + "}"


def _input(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    label_elem = _obj(prop.get("label"))
    label = _text(label_elem) if label_elem is not None else ""
    default_value = _string(prop.get("defaultValue")) or ""
    ph_elem = _obj(prop.get("placeholder"))
    placeholder = _text(ph_elem) if ph_elem is not None else ""
    if default_value:
        result = default_value + "___"
    elif placeholder:
        result = placeholder + "_____"
    else:
        result = "_____"
    if label:
        result = label + ": " + result
    return result.replace("_____", "...") if _string(prop.get("inputType")) == "multiline_text" else result


def _date_picker(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int, kind: str = "date") -> str:
    emoji, key = {"date": ("📅", "initialDate"), "time": ("🕐", "initialTime"), "datetime": ("📅", "initialDatetime")}.get(kind, ("📅", ""))
    value = _string(prop.get(key)) or "" if key else ""
    value = _normalize_time(value) if value else ""
    if not value:
        placeholder = "Select"
        ph_elem = _obj(prop.get("placeholder"))
        ph = _text(ph_elem) if ph_elem is not None else ""
        value = ph or placeholder
    return emoji + " " + value


def _checker(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    mark = "[x]" if _boolean(prop.get("checked")) else "[ ]"
    text_elem = _obj(prop.get("text"))
    text = _text(text_elem) if text_elem is not None else ""
    return mark + " " + text


def _image(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    alt = "Image"
    alt_elem = _obj(prop.get("alt"))
    if alt_elem is not None and _text(alt_elem):
        alt = _text(alt_elem)
    title_elem = _obj(prop.get("title"))
    if title_elem is not None and _text(title_elem):
        alt = _text(title_elem)
    result = "🖼️ " + alt
    image_id = _string(prop.get("imageID"))
    if image_id:
        origin_key, token = _image_key(ctx, image_id)
        if origin_key:
            result += f"(img_key:{origin_key})"
        elif token:
            result += f"(img_token:{token})"
        else:
            result += f"(img_key:{image_id})"
    return result


def _img_combination(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    img_list = _list(prop.get("imgList")) or []
    if not img_list:
        return ""
    result = f"🖼️ {len(img_list)} image(s)"
    keys: list[str] = []
    for img in img_list:
        im = _obj(img)
        image_id = _string(im.get("imageID")) if im is not None else None
        if image_id:
            origin_key, token = _image_key(ctx, image_id)
            keys.append(origin_key or token or image_id)
    return result + f"(keys:{','.join(keys)})" if keys else result


def _chart(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    title = "Chart"
    chart_type = ""
    spec = _obj(prop.get("chartSpec"))
    if spec is not None:
        title_obj = _obj(spec.get("title"))
        text = _string(title_obj.get("text")) if title_obj is not None else None
        if text:
            title = text
        ct = _string(spec.get("type"))
        if ct:
            chart_type = ct
            if ct in _CHART_TYPES:
                title = f"{title} ({_CHART_TYPES[ct]})" if title != "Chart" else _CHART_TYPES[ct]
    summary = _chart_summary(spec, chart_type)
    return "📊 " + title + (f"\nSummary: {summary}" if summary else "")


def _chart_summary(spec: dict[str, Any] | None, chart_type: str) -> str:
    if spec is None:
        return ""
    values: list[Any] = []
    data = spec.get("data")
    if isinstance(data, dict):
        values = _list(data.get("values")) or []
    elif isinstance(data, list):
        for series in data:
            sm = _obj(series)
            values.extend(_list(sm.get("values")) or [] if sm is not None else [])
    if not values:
        return ""
    if chart_type in ("line", "bar", "area"):
        x_field = _string(spec.get("xField")) or ""
        y_field = _string(spec.get("yField")) or ""
        if not x_field or not y_field:
            return f"{len(values)} data point(s)"
        parts = [f"{_fmt_v(vm.get(x_field))}:{_fmt_v(vm.get(y_field))}" for vm in (_obj(v) for v in values) if vm is not None]
        if parts:
            return ", ".join(parts)
    elif chart_type == "pie":
        cat_field = _string(spec.get("categoryField")) or ""
        val_field = _string(spec.get("valueField")) or ""
        if not cat_field or not val_field:
            return f"{len(values)} data point(s)"
        parts = [f"{_fmt_v(vm.get(cat_field))}:{_fmt_v(vm.get(val_field))}" for vm in (_obj(v) for v in values) if vm is not None]
        if parts:
            return ", ".join(parts)
    return f"{len(values)} data point(s)"


def _media(icon: str, primary: str, secondary: str) -> Callable[[_Ctx, dict[str, Any], dict[str, Any], str, int], str]:
    def convert(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
        file_id = _string(prop.get(primary)) or _string(prop.get(secondary)) or ""
        return icon + (f"(key:{file_id})" if file_id else "")

    return convert


def _table(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    columns = _list(prop.get("columns")) or []
    if not columns:
        return ""
    rows = _list(prop.get("rows")) or []
    names: list[str] = []
    keys: list[str] = []
    for column in columns:
        cm = _obj(column)
        if cm is None:
            continue
        name = _string(cm.get("name")) or ""
        names.append(_string(cm.get("displayName")) or name)
        keys.append(name)
    lines = ["| " + " | ".join(names) + " |", "|" + "------|" * len(names)]
    for row in rows:
        rm = _obj(row)
        if rm is None:
            continue
        cells = []
        for key in keys:
            cell = _obj(rm.get(key))
            cells.append(_cell_value(cell.get("data")) if cell is not None and cell.get("data") is not None else "")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def _cell_value(data: Any) -> str:
    if isinstance(data, str):
        texts = _go_map_texts(data)
        return ", ".join(texts) if texts else data
    if isinstance(data, float):
        return _fmt_f(data)
    if isinstance(data, list):
        return " ".join("「" + im["text"] + "」" for im in (_obj(v) for v in data) if im is not None and isinstance(im.get("text"), str))
    obj = _obj(data)
    return _text(obj) if obj is not None else ""


def _collapsible_panel(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    title = "Details"
    header = _obj(prop.get("header"))
    if header is not None and "title" in header:
        candidate = _text(header["title"])
        if candidate:
            title = candidate
    indicator = "▼" if _boolean(prop.get("expanded")) else "▶"
    out = indicator + " " + title + "\n"
    elements = _list(prop.get("elements"))
    if elements:
        content = _elements(ctx, elements, 1)
        out += "".join(f"    {line}\n" for line in content.split("\n") if line)
    return out + "▲"


def _form(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    elements = _list(prop.get("elements"))
    return "<form>\n" + _elements(ctx, elements or [], 0) + "\n</form>"


def _interactive_container(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    url = ""
    actions = _list(prop.get("actions")) or []
    first = _obj(actions[0]) if actions else None
    if first is not None and _string(first.get("type")) == "open_url":
        action_data = _obj(first.get("action"))
        url = (_string(action_data.get("url")) or "") if action_data is not None else ""
    out = "<clickable" + (f' url="{_escape_attr(url)}"' if url else "") + ">\n"
    elements = _list(prop.get("elements"))
    return out + _elements(ctx, elements or [], 0) + "\n</clickable>"


def _text_tag(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    text = _text(prop.get("text"))
    return "「" + text + "」" if text else ""


def _number_tag(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    text = _text(prop.get("text"))
    if not text:
        return ""
    url_obj = _obj(prop.get("url"))
    url = _string(url_obj.get("url")) if url_obj is not None else None
    return f"[{_escape_link_text(text)}]({url})" if url else text


def _link(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    content = _string(prop.get("content")) or ""
    content = content or "Link"
    url_obj = _obj(prop.get("url"))
    url = _string(url_obj.get("url")) if url_obj is not None else None
    return f"[{_escape_link_text(content)}]({url})" if url else content


def _emoji(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    key = _string(prop.get("key")) or ""
    return _EMOJI.get(key, f":{key}:")


def _local_datetime(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    ms = ""
    raw_ms = prop.get("milliseconds")
    if isinstance(raw_ms, str):
        ms = raw_ms
    elif isinstance(raw_ms, float):
        ms = str(int(raw_ms))
    if ms:
        formatted = _millis_iso(ms)
        if formatted:
            return formatted
    return _string(prop.get("fallbackText")) or ""


def _list_element(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    lines: list[str] = []
    for item in _list(prop.get("items")) or []:
        im = _obj(item)
        if im is None:
            continue
        level_num = _number(im.get("level"))
        level = int(level_num) if level_num is not None else 0
        order_num = _number(im.get("order"))
        order = floor(order_num) if order_num is not None else 0
        marker = f"{order}." if _string(im.get("type")) == "ol" else "-"
        elements = _list(im.get("elements"))
        if elements is not None:
            lines.append(f"{'  ' * level}{marker} {_md_elements(ctx, elements)}")
    return "\n".join(lines)


def _blockquote(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    content = _string(prop.get("content"))
    if content is None:
        elements = _list(prop.get("elements"))
        content = _md_elements(ctx, elements) if elements else ""
    return "\n".join("> " + line for line in content.split("\n")) if content else ""


def _code_block(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    language = _string(prop.get("language")) or ""
    code = []
    for line in _list(prop.get("contents")) or []:
        lm = _obj(line)
        for lc in _list(lm.get("contents")) or [] if lm is not None else []:
            cm = _obj(lc)
            text = _string(cm.get("content")) if cm is not None else None
            if text is not None:
                code.append(text)
    return f"```{language or 'plaintext'}\n{''.join(code)}```"


def _code_span(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    return "`" + (_string(prop.get("content")) or "") + "`"


def _heading(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    level_num = _number(prop.get("level"))
    level = min(6, max(1, int(level_num))) if level_num is not None else 1
    content = _string(prop.get("content"))
    if content is None:
        elements = _list(prop.get("elements"))
        content = _md_elements(ctx, elements) if elements else ""
    return "#" * level + " " + content


def _fallback_text(_ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    text_elem = _obj(prop.get("text"))
    if text_elem is not None:
        return _text(text_elem)
    elements = _list(prop.get("elements"))
    return _md_elements(_ctx, elements) if elements else ""


def _repeat(ctx: _Ctx, _elem: dict[str, Any], prop: dict[str, Any], _id: str, _depth: int) -> str:
    elements = _list(prop.get("elements"))
    return _elements(ctx, elements, 0) if elements else ""


def _unknown(_ctx: _Ctx, prop: dict[str, Any], _tag: str) -> str:
    for path in ("content", "text", "title", "label", "placeholder"):
        if path in prop:
            text = _text(prop[path])
            if text:
                return text
    elements = _list(prop.get("elements"))
    return _elements(_ctx, elements, 0) if elements else "[Unknown content]"


_Elements = dict[str, Callable[[_Ctx, dict[str, Any], dict[str, Any], str, int], str]]

_ELEMENTS: _Elements = {
    "plain_text": _plain_text,
    "text": _plain_text,
    "markdown": _markdown,
    "markdown_v1": _markdown_v1,
    "div": _div,
    "note": _note,
    "hr": _constant("---"),
    "br": _constant("\n"),
    "column_set": _column_set,
    "column": _column,
    "person": _person,
    "person_v1": _person_v1,
    "person_list": _person_list,
    "avatar": _avatar,
    "at": _at,
    "at_all": _constant("@everyone"),
    "button": _button,
    "actions": _actions,
    "action": _actions,
    "overflow": _overflow,
    "select_static": lambda ctx, _e, p, i, d: _select(ctx, _e, p, i, d),
    "select_person": lambda ctx, _e, p, i, d: _select(ctx, _e, p, i, d),
    "multi_select_static": lambda ctx, _e, p, i, d: _select(ctx, _e, p, i, d, multi=True),
    "multi_select_person": lambda ctx, _e, p, i, d: _select(ctx, _e, p, i, d, multi=True),
    "select_img": _select_img,
    "input": _input,
    "date_picker": lambda ctx, _e, p, i, d: _date_picker(ctx, _e, p, i, d, "date"),
    "picker_time": lambda ctx, _e, p, i, d: _date_picker(ctx, _e, p, i, d, "time"),
    "picker_datetime": lambda ctx, _e, p, i, d: _date_picker(ctx, _e, p, i, d, "datetime"),
    "checker": _checker,
    "img": _image,
    "image": _image,
    "img_combination": _img_combination,
    "table": _table,
    "chart": _chart,
    "audio": _media("🎵 Audio", "fileID", "audioID"),
    "video": _media("🎬 Video", "fileID", "videoID"),
    "collapsible_panel": _collapsible_panel,
    "form": _form,
    "interactive_container": _interactive_container,
    "text_tag": _text_tag,
    "number_tag": _number_tag,
    "link": _link,
    "emoji": _emoji,
    "local_datetime": _local_datetime,
    "list": _list_element,
    "blockquote": _blockquote,
    "code_block": _code_block,
    "code_span": _code_span,
    "heading": _heading,
    "fallback_text": _fallback_text,
    "repeat": _repeat,
    "card_header": _constant(""),
    "custom_icon": _constant(""),
    "standard_icon": _constant(""),
}


# ── Card assembly ─────────────────────────────────────────────────────────────


def _header_title(header: dict[str, Any]) -> str:
    prop = _obj(header.get("property"))
    if prop is not None and "title" in prop:
        return _text(prop["title"])
    return _text(header["title"]) if "title" in header else ""


def _header_subtitle(header: dict[str, Any]) -> str:
    prop = _obj(header.get("property"))
    if prop is not None and "subtitle" in prop:
        return _text(prop["subtitle"])
    return _text(header["subtitle"]) if "subtitle" in header else ""


def _header_tags(ctx: _Ctx, header: dict[str, Any]) -> str:
    prop = _obj(header.get("property")) or header
    tag_list = _list(prop.get("textTagList")) or []
    tags = [t for t in (_element(ctx, _obj(tag) or {}, 0) for tag in tag_list) if t]
    return " ".join(tags)


def _body_content(ctx: _Ctx, body: dict[str, Any]) -> str:
    elements: list[Any] = []
    prop = _obj(body.get("property"))
    if prop is not None:
        elements = _list(prop.get("elements")) or []
    if not elements:
        elements = _list(body.get("elements")) or []
    return _elements(ctx, elements, 0) if elements else ""


def _convert(ctx: _Ctx, json_card: str) -> str:
    card = _parse(json_card)
    if card is None:
        return UNABLE_TO_PARSE
    header = _obj(card.get("header"))
    title = subtitle = header_tags = ""
    if header is not None:
        title, subtitle, header_tags = _header_title(header), _header_subtitle(header), _header_tags(ctx, header)
    body = _obj(card.get("body"))
    body_content = _body_content(ctx, body) if body is not None else ""
    attrs = ""
    if title or subtitle:
        pairs = [f' title="{_escape_attr(title)}"' if title else "", f' subtitle="{_escape_attr(subtitle)}"' if subtitle else ""]
        attrs = "".join(pairs)
    lines = [f"<card{attrs}>"]
    if header_tags:
        lines.append(header_tags)
    if body_content:
        lines.append(body_content)
    lines.append("</card>")
    return "\n".join(lines)


def _convert_legacy(parsed: dict[str, Any]) -> str:
    texts: list[str] = []
    header = _obj(parsed.get("header"))
    if header is not None:
        title = _obj(header.get("title"))
        content = _string(title.get("content")) if title is not None else None
        if content:
            texts.append(f"**{content}**")
    body = _obj(parsed.get("body"))
    elements = _list(parsed.get("elements"))
    if elements is None and body is not None:
        elements = _list(body.get("elements"))
    _legacy_texts(elements or [], texts)
    return "\n".join(texts) if texts else UNPARSED


def _legacy_texts(elements: list[Any], out: list[str]) -> None:
    for el in elements:
        elem = _obj(el)
        if elem is None:
            continue
        tag = _string(elem.get("tag")) or ""
        if tag == "markdown":
            content = _string(elem.get("content"))
            if content is not None:
                out.append(content)
            continue
        if tag in ("div", "plain_text", "lark_md"):
            text = _obj(elem.get("text"))
            content = _string(text.get("content")) if text is not None else None
            if content:
                out.append(content)
            content = _string(elem.get("content"))
            if content:
                out.append(content)
        if tag == "column_set":
            for col in _list(elem.get("columns")) or []:
                cm = _obj(col)
                nested = _list(cm.get("elements")) if cm is not None else None
                if nested is not None:
                    _legacy_texts(nested, out)
        nested = _list(elem.get("elements"))
        if nested is not None:
            _legacy_texts(nested, out)


# ── Entry point ───────────────────────────────────────────────────────────────


def render_card(content: str, mentions: list | None = None, names: dict[str, str] | None = None) -> str:
    """Render one interactive message's body.content (usually {"json_card": "..."}) to the
    exact text lark-cli writes into the message view's content field, '<card>' wrapper and
    all. `mentions` is the raw mentions array (key/id/name); `names` maps open_id to a
    display name for person/@ rendering -- an id missing from it falls back to lark-cli's
    own sources (json_attachment maps, then the raw id). Malformed input renders lark-cli's
    placeholders instead of raising: '[interactive card]' for a broken body, and
    '<card>[Unable to parse card content]</card>' for a broken json_card."""
    parsed = _parse(content)
    if parsed is None:
        return _scrub(UNPARSED)
    json_card = parsed.get("json_card")
    if isinstance(json_card, str):
        attachment = None
        raw_attachment = parsed.get("json_attachment")
        if isinstance(raw_attachment, str):
            if raw_attachment:
                attachment = _parse(raw_attachment)
        elif isinstance(raw_attachment, dict):
            attachment = raw_attachment
        by_key = {key: item for item in (m if isinstance(m, dict) else {} for m in mentions or []) if (key := _string(item.get("key")))}
        result = _convert(_Ctx(attachment, by_key, dict(names or {})), json_card)
        return _scrub(result or UNPARSED)
    return _scrub(_convert_legacy(parsed))
