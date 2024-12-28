from yt.yt.orm.library.snapshot.codegen.name import SnapshotEntityName

from dataclasses import dataclass
import dacite
import pytest


def test_bool():
    assert not SnapshotEntityName()
    assert SnapshotEntityName(human_readable="foo bar")
    assert SnapshotEntityName(snake_case="foo_bar")
    assert SnapshotEntityName(pascal_case="FooBar")
    assert SnapshotEntityName(camel_case="fooBar")


def test_empty():
    name = SnapshotEntityName()
    assert name.human_readable is None
    assert name.snake_case is None
    assert name.pascal_case is None
    assert name.camel_case is None


def test_init_from_snake_case():
    name = SnapshotEntityName(snake_case="foo_bar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"


def test_init_from_invalid_snake_case():
    with pytest.raises(ValueError):
        SnapshotEntityName(snake_case="")
    with pytest.raises(ValueError):
        SnapshotEntityName(snake_case="FooBar")
    with pytest.raises(ValueError):
        SnapshotEntityName(snake_case="fooBar")
    with pytest.raises(ValueError):
        SnapshotEntityName(snake_case="foo_bar_!")


def test_init_from_pascal_case():
    name = SnapshotEntityName(pascal_case="FooBar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"

    name = SnapshotEntityName(pascal_case="XMLParser")
    assert name.human_readable == "xml parser"
    assert name.snake_case == "xml_parser"
    assert name.pascal_case == "XMLParser"
    assert name.camel_case == "xmlParser"

    name = SnapshotEntityName(pascal_case="IP6Address")
    assert name.human_readable == "ip6 address"
    assert name.snake_case == "ip6_address"
    assert name.pascal_case == "IP6Address"
    assert name.camel_case == "ip6Address"


def test_init_from_invalid_pascal_case():
    with pytest.raises(ValueError):
        SnapshotEntityName(pascal_case="")
    with pytest.raises(ValueError):
        SnapshotEntityName(pascal_case="foo_bar")
    with pytest.raises(ValueError):
        SnapshotEntityName(pascal_case="fooBar")
    with pytest.raises(ValueError):
        SnapshotEntityName(pascal_case="FooBar!")


def test_init_from_camel_case():
    name = SnapshotEntityName(camel_case="fooBar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"

    name = SnapshotEntityName(camel_case="fastXMLParser")
    assert name.human_readable == "fast xml parser"
    assert name.snake_case == "fast_xml_parser"
    assert name.pascal_case == "FastXMLParser"
    assert name.camel_case == "fastXMLParser"


def test_init_from_invalid_camel_case():
    with pytest.raises(ValueError):
        SnapshotEntityName(camel_case="")
    with pytest.raises(ValueError):
        SnapshotEntityName(camel_case="foo_bar")
    with pytest.raises(ValueError):
        SnapshotEntityName(camel_case="FooBar")
    with pytest.raises(ValueError):
        SnapshotEntityName(camel_case="fooBar!")


def test_init_from_human_readable():
    name = SnapshotEntityName(human_readable="foo bar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"


def test_init_override_human_readable():
    name = SnapshotEntityName(snake_case="foo_bar", human_readable="The Foo Bar")
    assert name.human_readable == "The Foo Bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"


def test_from_string_snake_case():
    name = SnapshotEntityName.from_string("foo_bar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"


def test_from_string_pascal_case():
    name = SnapshotEntityName.from_string("FooBar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"


def test_from_string_camel_case():
    name = SnapshotEntityName.from_string("fooBar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"


def test_from_string_human_readable():
    name = SnapshotEntityName.from_string("foo bar")
    assert name.human_readable == "foo bar"
    assert name.snake_case == "foo_bar"
    assert name.pascal_case == "FooBar"
    assert name.camel_case == "fooBar"


def test_from_string_single_token():
    name = SnapshotEntityName.from_string("foo")
    assert name.human_readable == "foo"
    assert name.snake_case == "foo"
    assert name.pascal_case == "Foo"
    assert name.camel_case == "foo"

    name = SnapshotEntityName.from_string("Foo")
    assert name.human_readable == "foo"
    assert name.snake_case == "foo"
    assert name.pascal_case == "Foo"
    assert name.camel_case == "foo"


def test_equality():
    a = SnapshotEntityName(human_readable="foo bar")
    b = SnapshotEntityName(snake_case="foo_bar")
    c = SnapshotEntityName(pascal_case="FooBar")
    d = SnapshotEntityName(camel_case="fooBar")

    assert a == b
    assert b == c
    assert c == d


def test_dict_key():
    d = dict()

    d[SnapshotEntityName(human_readable="foo bar")] = "value"

    assert SnapshotEntityName(snake_case="foo_bar") in d
    assert SnapshotEntityName(pascal_case="FooBar") in d
    assert SnapshotEntityName(camel_case="fooBar") in d


def test_dacite():
    src = dict(
        a="foo bar",
        b="foo_bar",
        c="FooBar",
        d="fooBar",
        e=dict(
            human_readable="foo bar",
            snake_case="foo_bar",
            pascal_case="FooBar",
            camel_case="fooBar",
        ),
    )

    @dataclass
    class MyData:
        a: SnapshotEntityName
        b: SnapshotEntityName
        c: SnapshotEntityName
        d: SnapshotEntityName
        e: SnapshotEntityName

    my_data = dacite.from_dict(
        MyData,
        src,
        config=dacite.Config(type_hooks={SnapshotEntityName: SnapshotEntityName.from_value}),
    )

    foo_bar = SnapshotEntityName(
        human_readable="foo bar",
        snake_case="foo_bar",
        pascal_case="FooBar",
        camel_case="fooBar",
    )

    assert my_data.a == foo_bar
    assert my_data.b == foo_bar
    assert my_data.c == foo_bar
    assert my_data.d == foo_bar
    assert my_data.e == foo_bar
