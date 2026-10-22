"""Card rendering is diffed against views lark-cli wrote earlier, so these pin the exact
tokens -- '<card>' wrapper, '[interactive card]' fallbacks -- that the mirror depends on."""

from json import dumps

from lark_fs.cards import render_card


def _body(card: object, **extra: object) -> str:
    """One interactive message body.content, with json_card nested as the API sends it."""
    return dumps({"json_card": dumps(card), "card_schema": 2, **extra})


def test_a_markdown_element_renders_inside_the_card_wrapper():
    """The wrapper is part of the recorded text: dropping it would break every diff."""
    content = _body({"body": {"elements": [{"tag": "markdown", "content": "hello **world**"}]}})
    assert render_card(content) == "<card>\nhello **world**\n</card>"


def test_a_header_title_becomes_an_attribute_not_a_body_line():
    """Escaping is lark-cli's attribute form; a quote left raw would close the attribute."""
    content = _body({"header": {"property": {"title": {"tag": "plain_text", "property": {"content": 'say "hi"'}}}}, "body": {"elements": [{"tag": "plain_text", "property": {"content": "body"}}]}})
    assert render_card(content) == '<card title="say \\"hi\\"">\nbody\n</card>'


def test_an_unparseable_body_falls_back_to_the_interactive_placeholder():
    """Never raises: the mirror records whatever lark-cli's fallback text was."""
    assert render_card("This message was recalled") == "[interactive card]"
    assert render_card('{"json_card": }') == "[interactive card]"


def test_an_unparseable_json_card_keeps_its_own_placeholder():
    """The nested card failing is a different fault than the body failing, and lark-cli says so."""
    assert render_card('{"json_card": "not json"}') == "<card>\n[Unable to parse card content]\n</card>"


def test_an_unknown_element_falls_back_to_its_nearest_text():
    """Unknown tags must hit convertUnknown's path, not vanish: the mirror greps their text."""
    content = _body({"body": {"elements": [{"tag": "frobnicator", "property": {"content": "mystery"}}]}})
    assert render_card(content) == "<card>\nmystery\n</card>"
    bare = _body({"body": {"elements": [{"tag": "frobnicator", "property": {}}]}})
    assert render_card(bare) == "<card>\n[Unknown content]\n</card>"


def test_control_characters_do_not_reach_the_mirror():
    """ANSI injection inside a card is recorded with the ESC byte dropped and the readable
    tail intact -- a bare ESC in the output would not reproduce what is on disk."""
    content = _body({"body": {"elements": [{"tag": "markdown", "property": {"content": "ok \x1b[31mred\x1b[39m"}}]}})
    assert render_card(content) == "<card>\nok [31mred[39m\n</card>"


def test_mentions_and_names_resolve_person_like_elements():
    """`names` is the caller's open_id -> display name cache; an id it misses keeps lark-cli's
    own fallback (the raw id), so a missing name never becomes a wrong name."""
    content = _body({"body": {"elements": [{"tag": "person", "property": {"userID": "ou_known"}}, {"tag": "person", "property": {"userID": "ou_unknown"}}]}})
    assert render_card(content, names={"ou_known": "Ada"}) == "<card>\nAda\nou_unknown\n</card>"
