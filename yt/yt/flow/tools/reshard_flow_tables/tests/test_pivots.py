from yt.wrapper import yson

from yt.yt.flow.tools.reshard_flow_tables.lib import key_sort_value


def sort_keys(keys):
    return sorted(keys, key=key_sort_value)


def test_uniform_keys_sort_by_value():
    keys = [["stream", "b", 2], ["stream", "a", 10], ["stream", "a", 2]]
    assert sort_keys(keys) == [["stream", "a", 2], ["stream", "a", 10], ["stream", "b", 2]]


def test_mixed_shapes_sort_lexicographically():
    # A queue source mid-release: the old layout is [stream, index], the new one is
    # [stream, identity, index]. Plain sorted() raises TypeError on the int-vs-str
    # collision; the YT order puts ints before strings.
    old_keys = [["stream", i] for i in range(2)]
    new_keys = [["stream", "0f3269c66a45507fbd6ada2f1385b17c", i] for i in range(2)]
    assert sort_keys(new_keys + old_keys) == old_keys + new_keys


def test_prefix_sorts_first():
    assert sort_keys([["stream", "a", 0], ["stream", "a"]]) == [["stream", "a"], ["stream", "a", 0]]


def test_type_order_matches_yt():
    # EValueType order: Int64 < Uint64 < Double < Boolean < String.
    columns = ["string", True, 1.5, yson.YsonUint64(7), -3]
    keys = [[column] for column in columns]
    assert sort_keys(keys) == [[-3], [yson.YsonUint64(7)], [1.5], [True], ["string"]]
