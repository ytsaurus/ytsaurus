import pytest
import yatest.common
import yt.yson as yson

from yt.yt.flow.library.python.client.state_unpack import unpack_state


@pytest.mark.parametrize("encoding", ["utf-8", None])
def test_unpack_golden_rows(encoding):
    with open(yatest.common.source_path("yt/yt/flow/tests/data/internal_state_rows.yson"), "rb") as stream:
        cases = yson.load(stream, encoding=encoding)
    key = (lambda name: name) if encoding else (lambda name: name.encode())
    for case in cases:
        row = case[key("row")]
        if case[key("expect_error")]:
            with pytest.raises((TypeError, ValueError)):
                unpack_state(row, encoding=encoding)
            continue
        original = yson.dumps(row, sort_keys=True)
        unpacked = unpack_state(row, encoding=encoding)
        assert yson.dumps(unpacked, sort_keys=True) == yson.dumps(case[key("expected_state")], sort_keys=True), case[
            key("name")
        ]
        assert yson.dumps(row, sort_keys=True) == original


@pytest.mark.parametrize("row", [None, False, 1, 1.0, [], b"row"])
def test_invalid_row_type(row):
    with pytest.raises((TypeError, ValueError)):
        unpack_state(row)


@pytest.mark.parametrize("value", [False, 0, "", b"", [], {}])
def test_ready_state_short_circuits(value):
    row = {"state": value, "compressed": 42, "format": False}
    assert unpack_state(row) is value


@pytest.mark.parametrize("column", ["state", "compressed", "compressed_patch", "format"])
@pytest.mark.parametrize("attributes", [{}, {"type": "null"}, {"unrelated": 42}])
def test_null_entities(column, attributes):
    row = {column: yson.to_yson_type(None, attributes=attributes)}
    assert isinstance(unpack_state(row), yson.YsonEntity)


@pytest.mark.parametrize("column", ["state", "compressed", "compressed_patch", "format"])
@pytest.mark.parametrize("value_type", ["min", "max", "int64", None, 42, [], {}])
def test_invalid_entity_types(column, value_type):
    row = {column: yson.to_yson_type(None, attributes={"type": value_type})}
    with pytest.raises(ValueError):
        unpack_state(row)
