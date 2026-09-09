"""Tests for in-place internal state tracking.

The value an accessor hands out is live: it is decoded once per key, every accessor for that key
returns the same object, and the changes made to it in place are collected at the end of the
request without a set() call.  A value that re-encodes to the bytes it arrived with, and a value
read through read_only(), produce no write.
"""

import pytest

from yt.yt.flow.library.python.companion.computation import Computation
from yt.yt.flow.library.python.companion.context import (
    ProtoStateAccessor,
    RawStateAccessor,
    ReadOnlyStateError,
    RequestContext,
    YsonStateAccessor,
)
from yt.yt.flow.library.python.companion.job import Job
from yt.yt.flow.library.python.companion.proto_mapper import map_process_batch_response
from yt.yt.flow.library.python.companion.row import (
    ColumnSchema,
    ExtendedMessage,
    Payload,
    PayloadBuilder,
    TableSchema,
)
from yt.yt.flow.library.python.companion.state import State, StatesHolder
from yt.yt.flow.library.python.companion.stream import (
    RawStream,
    StreamIdsMapping,
    StreamSpecs,
)
from yt.yt.flow.library.python.companion.test.proto.message_pb2 import TJoinState
from yt.yt.flow.library.python.companion.test_harness import ComputationHarness
from yt.yt.flow.library.python.companion.wire_protocol import WireProtocolReader
import yt.type_info as ti
import yt.yson as yson

_COMPUTATION_ID = "internal-state-computation"
_STATE_NAME = "counter-state"
_STREAM_ID = "words"

_KEY_SCHEMA = TableSchema([ColumnSchema("word", ti.String)])
_STREAM_SCHEMA = TableSchema([ColumnSchema("word", ti.String)])


def _key(word: str) -> Payload:
    return PayloadBuilder(_KEY_SCHEMA).set("word", word).finish()


def _holder() -> StatesHolder:
    return StatesHolder(_STATE_NAME, _KEY_SCHEMA, None)


def _seed(holder: StatesHolder, key: Payload, count: int) -> None:
    """Populate the holder as the incoming request does."""
    holder.load(key.row, State(state=yson.dumps({"count": count})))


def _modified(holder: StatesHolder) -> dict:
    """States the holder would send back, by the word of their key."""
    holder.collect_modified()
    return {Payload(row, _KEY_SCHEMA)["word"]: value for row, value in holder.modified_items()}


def _count(state: State) -> int:
    return yson.loads(state.state)["count"]


def _count_words(message, output, ctx):
    """Computation that only changes the state value in place."""
    ctx.state(_STATE_NAME, message).get_or_default({"count": 0})["count"] += 1


# ---------- YsonStateAccessor ----------


class TestYsonStateTracking:
    def test_mutation_is_written_back(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        YsonStateAccessor(key, holder).get()["count"] += 1

        assert _count(_modified(holder)["aa"]) == 2

    def test_reading_writes_nothing(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        assert YsonStateAccessor(key, holder).get()["count"] == 1

        assert _modified(holder) == {}

    def test_identical_value_writes_nothing(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        YsonStateAccessor(key, holder).get()["count"] = 1

        assert _modified(holder) == {}

    def test_get_or_default_writes_the_default(self):
        holder = _holder()
        key = _key("aa")

        YsonStateAccessor(key, holder).get_or_default({"count": 0})

        assert _count(_modified(holder)["aa"]) == 0

    def test_get_or_default_binds_the_default(self):
        holder = _holder()
        key = _key("aa")

        YsonStateAccessor(key, holder).get_or_default({"count": 0})["count"] = 7

        assert _count(_modified(holder)["aa"]) == 7

    def test_none_default_is_not_stored(self):
        holder = _holder()
        key = _key("aa")

        assert YsonStateAccessor(key, holder).get_or_default(None) is None

        assert _modified(holder) == {}

    def test_mutation_after_set_is_written_back(self):
        holder = _holder()
        key = _key("aa")

        value = {"count": 1}
        YsonStateAccessor(key, holder).set(value)
        value["count"] = 6

        assert _count(_modified(holder)["aa"]) == 6

    def test_same_key_gives_the_same_object(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        first = YsonStateAccessor(key, holder).get()
        second = YsonStateAccessor(key, holder).get()
        assert first is second

        first["count"] += 1
        second["count"] += 1

        assert len(_modified(holder)) == 1
        assert _count(_modified(holder)["aa"]) == 3

    def test_different_keys_are_tracked_independently(self):
        holder = _holder()
        first, second = _key("aa"), _key("ab")
        _seed(holder, first, 1)
        _seed(holder, second, 5)

        YsonStateAccessor(first, holder).get()["count"] += 1

        assert list(_modified(holder)) == ["aa"]
        assert _count(_modified(holder)["aa"]) == 2

    def test_clear_emits_a_reset(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        accessor = YsonStateAccessor(key, holder)
        accessor.clear()

        assert accessor.get() is None
        assert _modified(holder)["aa"].reset

    def test_get_or_default_after_clear_revives_the_state(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        accessor = YsonStateAccessor(key, holder)
        accessor.clear()
        accessor.get_or_default({"count": 0})["count"] = 5

        assert _count(_modified(holder)["aa"]) == 5

    def test_collect_modified_picks_up_a_later_change(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        value = YsonStateAccessor(key, holder).get()
        assert not holder.has_modified()

        value["count"] = 2
        assert _count(_modified(holder)["aa"]) == 2

        value["count"] = 3
        assert _count(_modified(holder)["aa"]) == 3

    def test_clear_discards_an_in_place_change(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        accessor = YsonStateAccessor(key, holder)
        accessor.get()["count"] = 2
        accessor.clear()

        assert _modified(holder)["aa"].reset

    def test_set_replaces_a_changed_value(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        accessor = YsonStateAccessor(key, holder)
        stale = accessor.get()
        stale["count"] = 2
        accessor.set({"count": 9})
        stale["count"] = 3

        assert _count(_modified(holder)["aa"]) == 9

    def test_keyless_accessor_stores_nothing(self):
        holder = _holder()
        accessor = YsonStateAccessor(None, holder)

        assert accessor.get() is None
        assert accessor.get_or_default({"count": 0})["count"] == 0

        assert _modified(holder) == {}

    def test_pre_encoded_bytes_are_stored_as_they_are(self):
        holder = _holder()
        key = _key("aa")

        YsonStateAccessor(key, holder).set(yson.dumps({"count": 4}))

        assert _count(_modified(holder)["aa"]) == 4


# ---------- read_only() ----------


class TestReadOnlyStateAccessor:
    def test_reading_writes_nothing(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        value = YsonStateAccessor(key, holder).read_only().get()
        assert value["count"] == 1
        value["count"] = 2

        assert _modified(holder) == {}

    def test_returns_the_object_the_writable_accessor_hands_out(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        accessor = YsonStateAccessor(key, holder)
        assert accessor.read_only().get() is accessor.get()

    def test_get_or_default_does_not_create_the_state(self):
        holder = _holder()
        key = _key("aa")

        accessor = YsonStateAccessor(key, holder).read_only()
        assert accessor.get_or_default({"count": 0}) == {"count": 0}

        assert accessor.get() is None
        assert _modified(holder) == {}

    def test_writes_are_rejected(self):
        holder = _holder()
        accessor = YsonStateAccessor(_key("aa"), holder).read_only()

        with pytest.raises(ReadOnlyStateError):
            accessor.set({"count": 1})
        with pytest.raises(ReadOnlyStateError):
            accessor.clear()

        assert accessor.read_only() is accessor

    def test_writable_accessor_is_unaffected(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        accessor = YsonStateAccessor(key, holder)
        accessor.read_only()
        accessor.get()["count"] += 1

        assert _count(_modified(holder)["aa"]) == 2


# ---------- RawStateAccessor ----------


class TestRawStateTracking:
    def test_reading_writes_nothing(self):
        holder = _holder()
        key = _key("aa")
        holder.load(key.row, State(state=b"\x01\x02"))

        assert RawStateAccessor(key, holder).get() == b"\x01\x02"

        assert _modified(holder) == {}

    def test_set_writes_the_bytes(self):
        holder = _holder()
        key = _key("aa")
        holder.load(key.row, State(state=b"\x01\x02"))

        RawStateAccessor(key, holder).set(b"\x03")

        assert _modified(holder)["aa"].state == b"\x03"

    def test_get_or_default_writes_the_default(self):
        holder = _holder()

        RawStateAccessor(_key("aa"), holder).get_or_default(b"\x00")

        assert _modified(holder)["aa"].state == b"\x00"

    def test_read_only_rejects_writes(self):
        holder = _holder()
        accessor = RawStateAccessor(_key("aa"), holder).read_only()

        with pytest.raises(ReadOnlyStateError):
            accessor.set(b"\x01")
        assert accessor.get_or_default(b"\x00") == b"\x00"

        assert _modified(holder) == {}


# ---------- ProtoStateAccessor ----------


class TestProtoStateTracking:
    @staticmethod
    def _seed_proto(holder: StatesHolder, key: Payload, show_time: int) -> None:
        state = TJoinState()
        state.show_time = show_time
        holder.load(key.row, State(state=state.SerializeToString()))

    @staticmethod
    def _show_time(state: State) -> int:
        message = TJoinState()
        message.ParseFromString(state.state)
        return message.show_time

    def test_mutation_is_written_back(self):
        holder = _holder()
        key = _key("aa")
        self._seed_proto(holder, key, 100)

        ProtoStateAccessor(key, holder, TJoinState).get().show_time = 200

        assert self._show_time(_modified(holder)["aa"]) == 200

    def test_reading_writes_nothing(self):
        holder = _holder()
        key = _key("aa")
        self._seed_proto(holder, key, 100)

        assert ProtoStateAccessor(key, holder, TJoinState).get().show_time == 100

        assert _modified(holder) == {}

    def test_get_or_default_writes_the_empty_message(self):
        holder = _holder()
        key = _key("aa")

        ProtoStateAccessor(key, holder, TJoinState).get_or_default().show_time = 300

        assert self._show_time(_modified(holder)["aa"]) == 300

    def test_read_only_writes_nothing(self):
        holder = _holder()
        key = _key("aa")
        self._seed_proto(holder, key, 100)

        ProtoStateAccessor(key, holder, TJoinState).read_only().get().show_time = 200

        assert _modified(holder) == {}

    def test_keyless_accessor_stores_nothing(self):
        holder = _holder()

        assert ProtoStateAccessor(None, holder, TJoinState).get_or_default().show_time == 0

        assert _modified(holder) == {}


# ---------- Accessors of different kinds on one state ----------


class TestMixedCodecs:
    """A value memoized by one accessor kind is not handed out by another."""

    def test_raw_read_after_yson_read(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        assert YsonStateAccessor(key, holder).get()["count"] == 1
        assert RawStateAccessor(key, holder).get() == yson.dumps({"count": 1})

        assert _modified(holder) == {}

    def test_yson_read_after_raw_read(self):
        holder = _holder()
        key = _key("aa")
        _seed(holder, key, 1)

        assert RawStateAccessor(key, holder).get() == yson.dumps({"count": 1})
        assert YsonStateAccessor(key, holder).get()["count"] == 1

        assert _modified(holder) == {}

    def test_same_proto_class_gives_the_same_message(self):
        holder = _holder()
        key = _key("aa")
        TestProtoStateTracking._seed_proto(holder, key, 100)

        first = ProtoStateAccessor(key, holder, TJoinState).get()
        second = ProtoStateAccessor(key, holder, TJoinState).get()
        assert first is second

        first.show_time = 200

        assert TestProtoStateTracking._show_time(_modified(holder)["aa"]) == 200


# ---------- End-to-end through the response proto ----------


def _proto_module():
    """Assemble the proto classes the response mapping needs, as the gRPC server does."""
    try:
        from yt.yt.flow.library.python.companion._proto_compat import ensure_proto_imports

        ensure_proto_imports()
        from yt.flow.library.cpp.companion.proto import (
            companion_service_pb2 as cs_pb2,
        )
        from yt.flow.library.cpp.common.proto import (
            message_pb2 as msg_pb2,
        )
    except ImportError:
        pytest.skip("Proto modules not available")

    class ProtoModule:
        pass

    module = ProtoModule()
    module.TResponseData = cs_pb2.TResponseData
    module.TMessageIdSuffix = cs_pb2.TMessageIdSuffix
    module.TNewTimer = cs_pb2.TNewTimer
    module.TState = cs_pb2.TState
    module.TStateItem = cs_pb2.TStateItem
    module.TMessage = msg_pb2.TMessage
    return module


class TestInternalStateFlush:
    """A computation that only changes the state value in place sends it back in the response."""

    @staticmethod
    def _stream_specs() -> StreamSpecs:
        mapping = StreamIdsMapping()
        mapping.add_mapping(_STREAM_ID, 0)
        return StreamSpecs(mapping, [RawStream(_STREAM_ID, _STREAM_SCHEMA)])

    @staticmethod
    def _message(word: str) -> ExtendedMessage:
        return ExtendedMessage(
            message_id=f"message-{word}",
            stream_id=_STREAM_ID,
            stream_spec_id=0,
            payload=PayloadBuilder(_STREAM_SCHEMA).set("word", word).finish(),
            key=_key(word),
        )

    def _process(self, function, words, holder):
        stream_specs = self._stream_specs()
        job = Job(
            job_id="test-job",
            computation_id=_COMPUTATION_ID,
            stream_specs=stream_specs,
            static_spec={"parameters": {"internal_states": [_STATE_NAME]}},
            group_by_schema=_KEY_SCHEMA,
        )
        request = RequestContext(
            job_id="test-job",
            request_id="test-request",
            computation_id=_COMPUTATION_ID,
            messages=[self._message(word) for word in words],
            stream_specs=stream_specs,
            internal_states={_STATE_NAME: holder},
            job=job,
        )
        response = Computation(_COMPUTATION_ID, function).do_process(request)
        return map_process_batch_response(stream_specs, response, _proto_module())

    @staticmethod
    def _counts(proto_state) -> dict:
        counts = {}
        for item in proto_state.stateItems:
            assert not item.reset
            key = Payload(WireProtocolReader(item.key).read_unversioned_row(), _KEY_SCHEMA)
            counts[key["word"]] = yson.loads(item.state)["count"]
        return counts

    def test_mutation_without_set_is_sent_back(self):
        data = self._process(_count_words, ["aa", "aa", "ab"], _holder())

        assert len(data.internal_states) == 1
        assert data.internal_states[0].name == _STATE_NAME
        assert self._counts(data.internal_states[0]) == {"aa": 2, "ab": 1}

    def test_reading_sends_nothing(self):
        holder = _holder()
        _seed(holder, _key("aa"), 3)
        seen = []

        def read_words(message, output, ctx):
            seen.append(ctx.state(_STATE_NAME, message).get())

        data = self._process(read_words, ["aa", "ab"], holder)

        assert [state["count"] if state is not None else None for state in seen] == [3, None]
        assert len(data.internal_states) == 0

    def test_read_only_change_is_not_sent_back(self):
        holder = _holder()
        _seed(holder, _key("aa"), 3)

        def touch_word(message, output, ctx):
            ctx.state(_STATE_NAME, message).read_only().get()["count"] += 1

        data = self._process(touch_word, ["aa"], holder)

        assert len(data.internal_states) == 0


class TestHarnessInPlaceState:
    """The test harness collects the changes made in place, as the response path does."""

    def test_mutation_without_set_is_visible(self):
        harness = ComputationHarness(
            _count_words,
            streams={_STREAM_ID: _STREAM_SCHEMA},
            key_schema=_KEY_SCHEMA,
            internal_states={_STATE_NAME},
        )
        key = harness.build_key(word="aa")
        messages = [harness.build_message(_STREAM_ID, key=key, word="aa") for _ in range(2)]

        with harness.processing(messages) as response:
            assert response.internal_state(_STATE_NAME, key)["count"] == 2
