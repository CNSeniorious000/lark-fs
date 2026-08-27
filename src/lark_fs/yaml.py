from re import IGNORECASE, VERBOSE, compile

type JSON = dict[str, JSON] | list[JSON] | tuple[JSON, ...] | str | int | float | bool | None


# A literal block cannot carry these: YAML forbids most C0 controls outright, and reads
# U+2028/2029 as line breaks, which would silently restructure the document. Such a string
# is written as a double-quoted scalar instead, where they survive as escapes -- dropping
# them would lose part of a message that no later run can fetch back.
RE_UNPRINTABLE = compile(r"[\x00-\x08\x0b-\x1f\x7f\u2028\u2029]")


def _quote_double(value: str) -> str:
    """Double-quoted style, the only YAML scalar that can carry an escape."""
    out = value.replace("\\", "\\\\").replace('"', '\\"')
    return '"' + RE_UNPRINTABLE.sub(lambda m: f"\\u{ord(m.group()):04x}", out).replace("\n", "\\n") + '"'


def readable_yaml_dumps(data: JSON):
    """
    Minimal YAML serializer optimized for readability.

    Uses literal block style (|) for all multi-line strings.
    Single-line strings are output without quotes when possible.

    Note: Generated output is for display only, not meant to be parsed.
    """
    lines: list[str] = []
    _serialize(data, lines, indent=0)
    return "".join(lines)


def _serialize(data: JSON, lines: list[str], indent: int):
    """Recursively serialize data into YAML format."""
    prefix = "  " * indent

    if isinstance(data, dict):
        _serialize_dict(data, lines, indent, prefix)
    elif isinstance(data, (list, tuple)):
        _serialize_list(data, lines, indent, prefix)
    elif isinstance(data, str):
        _serialize_string(data, lines, prefix)
    else:
        lines.append(f"{prefix}{_serialize_scalar(data)}\n")


def _serialize_dict(data: dict, lines: list[str], indent: int, prefix: str):
    """Serialize dictionary with key: value pairs."""
    if not data:
        lines.append(f"{prefix}{{}}\n")
        return

    for key, value in data.items():
        key_str = _serialize_scalar(key)

        if isinstance(value, (dict, list, tuple)):
            if not value:
                inline_repr = "{}" if isinstance(value, dict) else "[]"
                lines.append(f"{prefix}{key_str}: {inline_repr}\n")
            else:
                lines.append(f"{prefix}{key_str}:\n")
                _serialize(value, lines, indent + 1)
        elif isinstance(value, str) and "\n" in value and not RE_UNPRINTABLE.search(value):
            lines.append(f"{prefix}{key_str}:")
            _append_literal_block(value, lines, indent + 1)
        else:
            lines.append(f"{prefix}{key_str}: {_serialize_scalar(value)}\n")


def _serialize_list(data: list | tuple, lines: list[str], indent: int, prefix: str):
    """Serialize list/tuple with dash-style items."""
    if not data:
        lines.append(f"{prefix}[]\n")
        return

    for item in data:
        if isinstance(item, dict):
            if not item:
                lines.append(f"{prefix}- {{}}\n")
            else:
                _serialize_dict_in_list(item, lines, indent, prefix)
        elif isinstance(item, (list, tuple)):
            if not item:
                lines.append(f"{prefix}- []\n")
            else:
                lines.append(f"{prefix}-\n")
                _serialize(item, lines, indent + 1)
        elif isinstance(item, str) and "\n" in item and not RE_UNPRINTABLE.search(item):
            lines.append(f"{prefix}-")
            _append_literal_block(item, lines, indent + 1)
        else:
            lines.append(f"{prefix}- {_serialize_scalar(item)}\n")


def _serialize_dict_in_list(data: dict, lines: list[str], indent: int, prefix: str):
    """Serialize a non-empty dict as a list item, with first key inline."""
    lines.append(f"{prefix}-")
    item_prefix = "  " * (indent + 1)

    for i, (key, value) in enumerate(data.items()):
        key_str = _serialize_scalar(key)
        line_prefix = " " if i == 0 else item_prefix

        if isinstance(value, (dict, list, tuple)):
            # For empty collections, inline them
            if not value:
                inline_repr = "{}" if isinstance(value, dict) else "[]"
                lines.append(f"{line_prefix}{key_str}: {inline_repr}\n")
            else:
                lines.append(f"{line_prefix}{key_str}:\n")
                _serialize(value, lines, indent + 2)
        elif isinstance(value, str) and "\n" in value and not RE_UNPRINTABLE.search(value):
            lines.append(f"{line_prefix}{key_str}:")
            _append_literal_block(value, lines, indent + 2)
        else:
            lines.append(f"{line_prefix}{key_str}: {_serialize_scalar(value)}\n")


def _serialize_string(value: str, lines: list[str], prefix: str):
    """Serialize a string value (standalone, not as dict/list value)."""
    if "\n" in value and not RE_UNPRINTABLE.search(value):
        lines.append(f"{prefix}")
        _append_literal_block(value, lines, indent=1)
    else:
        lines.append(f"{prefix}{_serialize_scalar(value)}\n")


def _append_literal_block(value: str, lines: list[str], indent: int):
    """
    Append a multi-line string in literal block style (|).

    Chomping indicator selection:
    - |- (strip): If no trailing newline
    - | (clip): If has single trailing newline with content
    - |+ (keep): If has multiple trailing newlines, or only newlines (no content)

    An explicit indentation indicator is emitted whenever the block's first line begins
    with a space. YAML otherwise reads the indentation of that first line as the block's
    own, so every following line is *less* indented than the block claims and the block
    ends there -- mid-message, with the rest of the file read as the enclosing mapping.
    Two real messages did this, both starting with a space; the parse error lands several
    lines later, which is why it reads as corruption rather than as a quoting bug.
    """
    block_prefix = "  " * indent

    # Determine chomping indicator
    stripped_content = value.rstrip("\n")
    trailing_newlines = len(value) - len(stripped_content)
    if trailing_newlines == 0:
        # No trailing newlines: use strip
        chomp = "-"
        stripped_value = value
    elif trailing_newlines == 1 and stripped_content:
        # Single trailing newline with content: use clip (default)
        chomp = ""
        stripped_value = stripped_content
    else:
        # Multiple trailing newlines, or only newlines (no content): use keep
        chomp = "+"
        stripped_value = value

    explicit = str(len(block_prefix)) if stripped_value.startswith(" ") else ""
    lines.append(f" |{explicit}{chomp}\n")

    # Output content lines
    # Note: split("\n") on strings ending with \n produces a trailing empty
    # string that would add an extra newline when we append \n to each line
    content_lines = stripped_value.split("\n")
    if stripped_value.endswith("\n"):
        content_lines = content_lines[:-1]
    lines.extend(f"{block_prefix}{line}\n" for line in content_lines)


def _serialize_scalar(value: str | float | bool | None):
    """Convert scalar values to YAML string representation."""
    if value is None:
        return "~"
    elif isinstance(value, bool):
        return "true" if value else "false"
    elif isinstance(value, str):
        if RE_UNPRINTABLE.search(value) or "\n" in value:
            return _quote_double(value)  # the only style that can carry them
        if not RE_NEEDS_ESCAPE.search(value):
            return value
        if "\\" not in value and value.count('"') < value.count("'"):
            return f'"{value.replace('"', '\\"')}"'
        return f"'{value.replace("'", "''")}'"
    else:
        return str(value)


# Match patterns that require quotes for strings
RE_NEEDS_ESCAPE = compile(
    r"""
    (?:
      ^$  # empty string
      | ^(?:null|~|true|false|yes|no|on|off)$  # keywords
      | ^[-+]?(?:[0-9][0-9_]*)?$  # integers
      | ^[-+]?(?:[0-9][0-9_]*)?\.[0-9_]*$  # floats
      | ^[-+]?(?:[0-9][0-9_]*)?(?:\.[0-9_]*)?[eE][-+]?[0-9]+$  # scientific notation
      | ^\.inf$|^\.nan$  # special floats
      | ^\s|\s$  # leading or trailing whitespace
      | [:\[\]{},&*#?|<>!`@%'\"\t\r\n-]  # special characters, % included: it opens a directive
    )
    """,
    IGNORECASE | VERBOSE,
)


__all__ = "JSON", "readable_yaml_dumps"
