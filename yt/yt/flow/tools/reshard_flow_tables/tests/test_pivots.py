import argparse

import pytest

from yt.wrapper import yson

from yt.yt.flow.tools.reshard_flow_tables.lib import (
    key_sort_value,
    plan_key_visitor_states_table,
    plan_leases_table,
    plan_partition_table,
    plan_pipeline_tables,
)


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


def test_binary_source_keys_sort_as_bytes():
    # A source key that is not valid UTF-8 arrives from the flow view as a YsonStringProxy, which is
    # neither str nor bytes.
    binary = yson.make_byte_key(b"\xff\xfe")
    assert sort_keys([["stream", binary], ["stream", "a"], ["stream", 1]]) == [
        ["stream", 1],
        ["stream", "a"],
        ["stream", binary],
    ]


def test_type_order_matches_yt():
    # EValueType order: Int64 < Uint64 < Double < Boolean < String.
    columns = ["string", True, 1.5, yson.YsonUint64(7), -3]
    keys = [[column] for column in columns]
    assert sort_keys(keys) == [[-3], [yson.YsonUint64(7)], [1.5], [True], ["string"]]


class FakeClient:
    def __init__(self, rows=None, error=None):
        self.rows = rows
        self.error = error
        self.queries = []

    def select_rows(self, query):
        self.queries.append(query)
        if self.error is not None:
            raise self.error
        return self.rows


FULL_WIDTH = ("//pipeline/leases", {"tablet_count": 6, "uniform": True})


def test_empty_leases_table_is_planned_to_a_single_tablet():
    client = FakeClient([])

    assert plan_leases_table(client, ["a", "b"], "//pipeline", 3) == (
        "//pipeline/leases",
        {"tablet_count": 1, "uniform": True},
    )
    assert client.queries == ["* FROM [//pipeline/leases] LIMIT 1"]


def test_populated_leases_table_is_planned_to_the_full_width():
    client = FakeClient([{"key": "", "subkey": "expiration"}])

    assert plan_leases_table(client, ["a", "b"], "//pipeline", 3) == FULL_WIDTH


def test_explicitly_named_leases_table_is_planned_to_the_full_width_even_when_empty():
    client = FakeClient([])

    assert plan_leases_table(client, ["a", "b"], "//pipeline", 3, infer_unused=False) == FULL_WIDTH
    assert client.queries == []


def test_unreadable_leases_table_is_planned_to_the_full_width():
    client = FakeClient(error=RuntimeError("no in-sync replicas"))

    assert plan_leases_table(client, ["a", "b"], "//pipeline", 3) == FULL_WIDTH


# A pipeline whose spec carries no computations: the width of a partition table is a multiple of
# their number, so planning one at all yields zero tablets, and the reshard fails on it later with
# "Tablet count must be positive".
def test_partition_table_of_a_pipeline_without_computations_is_skipped():
    assert plan_partition_table([], "//pipeline/partition_states", 20) is None


def test_leases_table_of_a_pipeline_without_computations_is_skipped():
    client = FakeClient([{"key": "", "subkey": "expiration"}])

    assert plan_leases_table(client, [], "//pipeline", 20) is None


def test_key_visitor_states_pivots_are_keyed_by_computation_and_stream():
    request = plan_key_visitor_states_table([("comp_b", "s1"), ("comp_a", "s2"), ("comp_a", "s1")], "//pipeline", 2)

    assert request.table == "//pipeline/key_visitor_states"
    assert request.computation_ids == ("comp_a", "comp_b")
    half = yson.YsonList([yson.YsonUint64(2**63)])
    assert request.parameters["pivot_keys"] == [
        [],
        ["comp_a", "s1", half],
        ["comp_a", "s2"],
        ["comp_a", "s2", half],
        ["comp_b"],
        ["comp_b", "s1", half],
    ]


def test_key_visitor_states_hash_pivots_are_uint64():
    request = plan_key_visitor_states_table([("comp", "visit")], "//pipeline", 4)

    hashes = [key[2][0] for key in request.parameters["pivot_keys"][1:]]
    assert hashes == [2**62, 2**63, 3 * 2**62]
    assert all(isinstance(value, yson.YsonUint64) for value in hashes)


def test_key_visitor_states_table_of_a_pipeline_without_such_streams_is_skipped():
    assert plan_key_visitor_states_table([], "//pipeline", 20) is None


class FakeSpecClient:
    def __init__(self, computations):
        self.computations = computations

    def get_pipeline_spec(self, path):
        return {"spec": {"computations": self.computations}}

    def get_flow_view(self, path, view, cache=False):
        return {}

    def select_rows(self, query):
        return []


def make_key_visitor_computation(first_column_type):
    return {
        "input_stream_ids": [],
        "output_stream_ids": [],
        "timer_streams": {},
        "source_streams": {},
        "key_visitor_streams": {"visit": {}},
        "group_by_schema": [{"name": "hash", "type": first_column_type}],
    }


def plan_key_visitor_states(computations, table="key_visitor_states"):
    args = argparse.Namespace(pipeline_path="//pipeline", tablet_count=2, table=table)
    return plan_pipeline_tables(FakeSpecClient(computations), args)


def key_visitor_states_plans(plans):
    return [plan for plan in plans if getattr(plan, "table", None) == "//pipeline/key_visitor_states"]


def test_key_visitor_streams_are_planned_from_the_spec():
    (request,) = plan_key_visitor_states({"comp": make_key_visitor_computation("uint64")})

    assert request.table == "//pipeline/key_visitor_states"
    assert request.computation_ids == ("comp",)
    half = yson.YsonList([yson.YsonUint64(2**63)])
    assert request.parameters["pivot_keys"] == [[], ["comp", "visit", half]]


def test_spec_without_key_visitor_streams_field_is_planned():
    computation = make_key_visitor_computation("uint64")
    del computation["key_visitor_streams"]

    plans = plan_key_visitor_states({"comp": computation}, table=None)

    assert plans
    assert key_visitor_states_plans(plans) == []


def test_key_visitor_streams_require_a_uint64_hash_column():
    with pytest.raises(AssertionError, match="uint64"):
        plan_key_visitor_states({"comp": make_key_visitor_computation("string")})


def test_key_visitor_hash_column_is_checked_only_when_the_table_is_planned():
    assert plan_key_visitor_states({"comp": make_key_visitor_computation("string")}, table="timers") == []


def test_key_visitor_states_streams_get_exactly_the_requested_tablet_count():
    request = plan_key_visitor_states_table([("comp", "visit")], "//pipeline", 10)

    hashes = [pivot[2][0] for pivot in request.parameters["pivot_keys"][1:]]
    assert hashes == [(2**64 * k) // 10 for k in range(1, 10)]
