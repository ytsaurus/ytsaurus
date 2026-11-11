"""Profile-state processing through the public Python harness."""

import pytest
from google.protobuf.message import Message as ProtoMessage

from yt.yt.flow.library.python.companion.computation import RowFunction
from yt.yt.flow.library.python.companion.context import ReadOnlyExternalStateError
from yt.yt.flow.library.python.companion.state import ProtoExternalState
from yt.yt.flow.library.python.companion.test.proto.message_pb2 import TJoinedAction, TJoinState
from yt.yt.flow.library.python.companion.test_harness import ComputationHarness, schema


def harness(function, **kwargs):
    return ComputationHarness(
        function,
        streams={"input": schema(id="string")},
        key_schema=schema(id="string"),
        **kwargs,
    )


def test_create_read_modify_and_carry_to_next_request():
    def increment(message, output, ctx):
        accessor = ctx.proto_external_state("/profile", message, TJoinState)
        value = accessor.get_or_default()
        value.show_time += 1
        accessor.set(value)
        value.show_time += 100
        assert ctx.proto_external_state("/profile", message, TJoinState).get().show_time < 100

    h = harness(increment, proto_external_states={"/profile": TJoinState})
    key = h.build_key(id="one")
    message = h.build_message("input", key=key, id="one")
    with h.processing([message, message]) as response:
        assert response.external_state_proto("/profile", key, TJoinState).show_time == 2
        assert response.has_external_state("/profile")
        previous = response.external_state_raw("/profile", key)
    with h.processing([message], external_states={"/profile": {key: previous}}) as response:
        assert response.external_state_proto("/profile", key, TJoinState).show_time == 3
    assert TJoinState.FromString(previous.state).show_time == 2


@pytest.mark.parametrize("initial", [None, TJoinState(), TJoinState(show_time=7)])
def test_read_and_in_place_changes_without_set_do_not_write(initial):
    def read(message, output, ctx):
        state = ctx.proto_external_state("/profile", message, TJoinState)
        assert (state.get() is None) == (initial is None)
        value = state.get_or_default()
        value.show_time = 99
        assert state.get_or_default().show_time == (initial.show_time if initial is not None else 0)

    h = harness(read, proto_external_states={"/profile": TJoinState})
    key = h.build_key(id="one")
    preload = {"/profile": {key: initial}} if initial is not None else {}
    with h.processing([h.build_message("input", key=key, id="one")], external_states=preload) as response:
        assert not response.has_external_state("/profile")
        assert response.external_state_proto("/profile", key, TJoinState) == initial


@pytest.mark.parametrize("operations,reset", [("set", False), ("set clear", True), ("clear set", False)])
def test_empty_profile_and_reset_are_distinct(operations, reset):
    def process(message, output, ctx):
        state = ctx.proto_external_state("/profile", message, TJoinState)
        for operation in operations.split():
            if operation == "set":
                state.set(TJoinState())
            else:
                state.clear()

    h = harness(process, proto_external_states={"/profile": TJoinState})
    key = h.build_key(id="one")
    with h.processing([h.build_message("input", key=key, id="one")]) as response:
        assert response.has_external_state("/profile")
        assert response.external_state_is_reset("/profile", key) == reset
        value = response.external_state_proto("/profile", key, TJoinState)
        assert (value is None) == reset
        if not reset:
            assert response.external_state_raw("/profile", key).state == b""


@pytest.mark.parametrize("present", [False, True])
def test_joined_reads_are_isolated_and_can_feed_mutable_state(present):
    def join(message, output, ctx):
        joined = ctx.joined_proto_external_state("/joined", message, TJoinState)
        if not present:
            assert joined.get() is None
        value = joined.get_or_default()
        value.show_time += 1
        ctx.proto_external_state("/profile", message, TJoinState).set(value)
        with pytest.raises(ReadOnlyExternalStateError):
            joined.set(value)
        with pytest.raises(ReadOnlyExternalStateError):
            joined.clear()

    h = harness(
        join, proto_external_states={"/profile": TJoinState}, joined_proto_external_states={"/joined": TJoinState}
    )
    one, two = h.build_key(id="one"), h.build_key(id="two")
    messages = [h.build_message("input", key=key, id=name) for key, name in [(one, "one"), (two, "two")]]
    original = TJoinState(show_time=7)
    preload = {"/joined": {one: original}} if present else {}
    with h.processing(messages, joined_external_states=preload) as response:
        assert response.external_state_proto("/profile", one, TJoinState).show_time == (8 if present else 1)
        assert response.external_state_proto("/profile", two, TJoinState).show_time == 1
        assert response.joined_external_state_proto("/joined", one, TJoinState) == (original if present else None)
        assert response.joined_external_state_proto("/joined", two, TJoinState) is None
        assert not response.joined_external_state_holder("/joined").collect_modified()
        assert not response.has_external_state("/joined")
        with pytest.raises(ValueError, match="not in the row format"):
            response.joined_external_state("/joined", one)


def test_timer_and_visit_access_same_profile_contract():
    class Function(RowFunction):
        def on_message(self, message, output, ctx):
            ctx.proto_external_state("/profile", message, TJoinState).set(TJoinState(show_time=1))

        def on_timer(self, timer, output, ctx):
            state = ctx.proto_external_state("/profile", timer, TJoinState)
            value = state.get()
            value.show_time += 2
            state.set(value)

        def on_visit(self, visit, output, ctx):
            state = ctx.proto_external_state("/profile", visit, TJoinState)
            value = state.get()
            value.show_time += 4
            state.set(value)

    h = harness(Function(), proto_external_states={"/profile": TJoinState})
    key = h.build_key(id="one")
    with h.processing(
        [h.build_message("input", key=key, id="one")],
        timers=[h.build_timer(key=key, trigger_timestamp=1)],
        visits=[h.build_visit(key=key)],
    ) as response:
        assert response.external_state_proto("/profile", key, TJoinState).show_time == 7


def test_row_and_proto_states_coexist():
    def process(message, output, ctx):
        row = ctx.external_state("/row", message)
        row.set(row.to_builder().set("count", 4).finish())
        ctx.proto_external_state("/profile", message, TJoinState).set(TJoinState(show_time=7))

    h = harness(
        process, external_states={"/row": schema(count="uint64")}, proto_external_states={"/profile": TJoinState}
    )
    key = h.build_key(id="one")
    with h.processing([h.build_message("input", key=key, id="one")]) as response:
        assert response.external_state("/row", key)["count"] == 4
        assert response.external_state_proto("/profile", key, TJoinState).show_time == 7
        with pytest.raises(ValueError, match="not in the row format"):
            response.external_state("/profile", key)


def test_reject_conflicting_declarations_and_wrong_preload_type():
    def noop(message, output, ctx):
        pass

    with pytest.raises(ValueError, match="both row and proto"):
        harness(
            noop, external_states={"/profile": schema(count="uint64")}, proto_external_states={"/profile": TJoinState}
        )
    for proto_class in (TJoinState(), ProtoMessage):
        with pytest.raises(TypeError, match="message class"):
            harness(noop, proto_external_states={"/profile": proto_class})
    h = harness(noop, proto_external_states={"/profile": TJoinState})
    key = h.build_key(id="one")
    with pytest.raises(TypeError, match="TJoinState"):
        with h.processing([], external_states={"/profile": {key: TJoinedAction()}}):
            pass


@pytest.mark.parametrize(
    "proto_class,error,match",
    [(TJoinedAction, ValueError, "Proto type mismatch"), (ProtoMessage, TypeError, "message class")],
)
def test_reject_wrong_accessor_type_even_for_missing_profile(proto_class, error, match):
    def process(message, output, ctx):
        ctx.proto_external_state("/profile", message, proto_class)

    h = harness(process, proto_external_states={"/profile": TJoinState})
    key = h.build_key(id="one")
    with pytest.raises(error, match=match):
        with h.processing([h.build_message("input", key=key, id="one")]):
            pass


def test_carried_reset_remains_absent_until_explicit_set():
    def process(message, output, ctx):
        state = ctx.proto_external_state("/profile", message, TJoinState)
        assert state.get() is None
        state.get_or_default().show_time = 100

    h = harness(process, proto_external_states={"/profile": TJoinState})
    key = h.build_key(id="one")
    with h.processing(
        [h.build_message("input", key=key, id="one")],
        external_states={"/profile": {key: ProtoExternalState(reset=True)}},
    ) as response:
        assert not response.has_external_state("/profile")
        assert response.external_state_proto("/profile", key, TJoinState) is None
