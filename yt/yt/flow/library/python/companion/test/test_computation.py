"""Tests for computation classes."""

import pytest

from yt.yt.flow.library.python.companion.computation import (
    BatchFunction,
    Computation,
    RootCollector,
    RowFunction,
    SourceComputation,
)
from yt.yt.flow.library.python.companion.context import (
    ReadOnlyExternalStateError,
)
from yt.yt.flow.library.python.companion.row import (
    ExtendedMessage,
    Message,
    PayloadBuilder,
    Timer,
)
from yt.yt.flow.library.python.companion.test_harness import (
    ComputationHarness,
    schema,
)


class PassthroughRowFunction(RowFunction):
    """Passes every message through to output."""

    def on_message(self, message, output, ctx):
        output.add_message(
            Message(
                message_id=message.message_id,
                stream_id=message.stream_id,
                payload=message.payload,
            )
        )


class FilteringRowFunction(RowFunction):
    """Only passes messages with stream_id == 'keep'."""

    def on_message(self, message, output, ctx):
        if message.stream_id == "keep":
            output.add_message(
                Message(
                    message_id=message.message_id,
                    stream_id=message.stream_id,
                    payload=message.payload,
                )
            )


class TimerRowFunction(RowFunction):
    """Adds a timer for each message."""

    def on_message(self, message, output, ctx):
        output.add_timer(trigger_timestamp=1000, event_timestamp=500, stream_id="timer-stream")

    def on_timer(self, timer, output, ctx):
        output.add_message(Message(message_id="from-timer", stream_id="output"))


class SimpleBatchFunction(BatchFunction):
    """Collects all messages into a single output."""

    def on_messages(self, messages, output, ctx):
        for m in messages:
            output.add_message(Message(message_id=m.message_id, stream_id="out"))


class DistributeRowFunction(RowFunction):
    """Emits every message twice: once distributed, once not."""

    def on_message(self, message, output, ctx):
        output.add_message(
            Message(message_id=message.message_id, stream_id="out"),
            distribute=True,
        )
        output.add_message(
            Message(message_id="dup_" + message.message_id, stream_id="out"),
            distribute=False,
        )


class TestRootCollector:
    def test_set_parent_ids_string(self):
        collector = RootCollector()
        output = collector.set_parent_ids("msg-1")
        output.add_message(Message(message_id="out-1"))

        results = collector.collect_results()
        assert len(results) == 1
        assert results[0].parent_ids == ["msg-1"]
        assert len(results[0].messages) == 1

    def test_set_parent_ids_list(self):
        collector = RootCollector()
        output = collector.set_parent_ids(["msg-1", "msg-2"])
        output.add_message(Message(message_id="out-1"))

        results = collector.collect_results()
        assert results[0].parent_ids == ["msg-1", "msg-2"]

    def test_multiple_collectors(self):
        collector = RootCollector()
        out1 = collector.set_parent_ids("msg-1")
        out1.add_message(Message(message_id="out-1"))
        out2 = collector.set_parent_ids("msg-2")
        out2.add_message(Message(message_id="out-2"))

        results = collector.collect_results()
        assert len(results) == 2

    def test_add_timer(self):
        collector = RootCollector()
        output = collector.set_parent_ids("msg-1")
        output.add_timer(trigger_timestamp=100, event_timestamp=50, stream_id="s1")

        results = collector.collect_results()
        assert len(results[0].timers) == 1
        assert results[0].timers[0].trigger_timestamp == 100


class TestComputation:
    def _make_request_ctx(self, messages=None, timers=None):
        """Helper to create a minimal RequestContext."""
        from yt.yt.flow.library.python.companion.context import RequestContext
        from yt.yt.flow.library.python.companion.job import Job
        from yt.yt.flow.library.python.companion.stream import (
            StreamIdsMapping,
            StreamSpecs,
            RawStream,
        )

        mapping = StreamIdsMapping({"input": 0, "output": 1})
        specs = StreamSpecs(mapping, [RawStream("input"), RawStream("output")])

        job = Job(
            job_id="test-job",
            computation_id="test-comp",
            stream_specs=specs,
            static_spec={},
        )

        return RequestContext(
            job_id="test-job",
            request_id="test-req",
            computation_id="test-comp",
            messages=messages or [],
            timers=timers or [],
            stream_specs=specs,
            job=job,
        )

    def test_row_function_passthrough(self):
        comp = Computation(
            computation_id="test",
            process_function=PassthroughRowFunction(),
        )
        assert comp.function_type == "Row"
        assert comp.computation_type == "Transform"

        messages = [
            ExtendedMessage(message_id="m1", stream_id="input"),
            ExtendedMessage(message_id="m2", stream_id="input"),
        ]
        ctx = self._make_request_ctx(messages=messages)
        response = comp.do_process(ctx)

        assert len(response.transform_results) == 2
        assert response.transform_results[0].messages[0].message_id == "m1"
        assert response.transform_results[1].messages[0].message_id == "m2"

    def test_row_function_filtering(self):
        comp = Computation(
            computation_id="test",
            process_function=FilteringRowFunction(),
        )

        messages = [
            ExtendedMessage(message_id="m1", stream_id="keep"),
            ExtendedMessage(message_id="m2", stream_id="drop"),
            ExtendedMessage(message_id="m3", stream_id="keep"),
        ]
        ctx = self._make_request_ctx(messages=messages)
        response = comp.do_process(ctx)

        # Only messages with stream_id="keep" produce output.
        non_empty = [r for r in response.transform_results if r.messages]
        assert len(non_empty) == 2

    def test_row_function_with_timers(self):
        comp = Computation(
            computation_id="test",
            process_function=TimerRowFunction(),
        )

        messages = [ExtendedMessage(message_id="m1", stream_id="input")]
        timers = [Timer(message_id="t1")]
        ctx = self._make_request_ctx(messages=messages, timers=timers)
        response = comp.do_process(ctx)

        # Message produces a timer, timer produces a message.
        results = response.transform_results
        assert any(r.timers for r in results)
        assert any(r.messages for r in results)

    def test_batch_function(self):
        comp = Computation(
            computation_id="test",
            process_function=SimpleBatchFunction(),
        )
        assert comp.function_type == "Batch"

        messages = [
            ExtendedMessage(message_id="m1"),
            ExtendedMessage(message_id="m2"),
            ExtendedMessage(message_id="m3"),
        ]
        ctx = self._make_request_ctx(messages=messages)
        response = comp.do_process(ctx)

        # Batch produces one group with all parent IDs.
        assert len(response.transform_results) == 1
        assert len(response.transform_results[0].messages) == 3

    def test_empty_messages(self):
        comp = Computation(
            computation_id="test",
            process_function=PassthroughRowFunction(),
        )
        ctx = self._make_request_ctx(messages=[])
        response = comp.do_process(ctx)
        assert len(response.transform_results) == 0

    def test_plain_function_row(self):
        """Test that a plain function with 'message' param works as RowFunction."""

        def my_fn(message, output, ctx):
            output.add_message(Message(message_id=message.message_id, stream_id="out"))

        comp = Computation(computation_id="test", process_function=my_fn)
        assert comp.function_type == "Row"

        messages = [ExtendedMessage(message_id="m1", stream_id="input")]
        ctx = self._make_request_ctx(messages=messages)
        response = comp.do_process(ctx)
        assert len(response.transform_results) == 1
        assert response.transform_results[0].messages[0].message_id == "m1"

    def test_plain_function_batch(self):
        """Test that a plain function with 'messages' param works as BatchFunction."""

        def my_batch_fn(messages, output, ctx):
            for m in messages:
                output.add_message(Message(message_id=m.message_id, stream_id="out"))

        comp = Computation(computation_id="test", process_function=my_batch_fn)
        assert comp.function_type == "Batch"

        messages = [
            ExtendedMessage(message_id="m1"),
            ExtendedMessage(message_id="m2"),
        ]
        ctx = self._make_request_ctx(messages=messages)
        response = comp.do_process(ctx)
        assert len(response.transform_results) == 1
        assert len(response.transform_results[0].messages) == 2

    def test_distribute_flag_recorded(self):
        comp = Computation(
            computation_id="test",
            process_function=DistributeRowFunction(),
        )

        messages = [ExtendedMessage(message_id="m1", stream_id="input")]
        ctx = self._make_request_ctx(messages=messages)
        response = comp.do_process(ctx)

        # The distribute flag is recorded per message in lockstep with messages.
        result = response.transform_results[0]
        assert [m.message_id for m in result.messages] == ["m1", "dup_m1"]
        assert result.distribute == [True, False]


class TestProcessFunctionRequired:
    def test_none_process_function_still_invalid(self):
        with pytest.raises(ValueError):
            Computation(computation_id="c", process_function=None)
        with pytest.raises(ValueError):
            SourceComputation(computation_id="src", process_function=None)


# ---------- Joiner: read-only joined external state ----------

_WORD_JOINER = "/word-state-external"


class JoinerCounter(RowFunction):
    """Reads a joined external counter and emits {word, count}.

    Mirrors the Java JoinerCounterTest: the writer computation's per-key state is
    surfaced here through the join; this computation only reads it and never writes back.
    """

    def on_message(self, message, output, ctx):
        joined = ctx.joined_external_state(_WORD_JOINER, message)
        count = joined.get("count") or 0
        word = message.payload["word"]
        builder = ctx.message_builder("out")
        builder.set("word", word)
        builder.set("count", count)
        output.add_message(builder.finish())


class TestJoinerCounter:
    def test_writer_value_surfaced_and_no_write_back(self):
        h = ComputationHarness(
            JoinerCounter(),
            streams={"words": schema(word="string"), "out": schema(word="string", count="int64")},
            key_schema=schema(word="string"),
            joined_external_states={_WORD_JOINER: schema(count="int64")},
        )
        key = h.build_key(word="hello")
        msg = h.build_message("words", key=key, word="hello")
        joined_value = PayloadBuilder(schema(count="int64")).set("count", 7).finish()

        with h.processing([msg], joined_external_states={_WORD_JOINER: {key: joined_value}}) as r:
            assert len(r.messages) == 1
            assert r.messages[0].payload["count"] == 7

            # Joiner never writes back: no external_states entry in the response, and the
            # joined holder was only loaded, never set.
            assert not r.has_external_state(_WORD_JOINER)
            assert r.joined_external_state_holder(_WORD_JOINER).has_modified() is False


# ---------- Key-visitor onVisit eviction ----------

_USER_STATE = "/user-state-external"


class EvictTester(RowFunction):
    """Writes external state on message; on visit, conditionally evicts it.

    Mirrors the Java EvictTester: ``on_visit`` reads the writable external state, returns
    early if empty, emits a visits message, then clears the state when ``parameters['evict']``
    is truthy. Eviction is entirely user-side; the SDK adds nothing for it.
    """

    def on_message(self, message, output, ctx):
        state = ctx.external_state(_USER_STATE, message)
        builder = state.to_builder()
        builder.set("seen", (state.get("seen") or 0) + 1)
        state.set(builder.finish())

    def on_visit(self, visit, output, ctx):
        state = ctx.external_state(_USER_STATE, visit)
        if state.get("seen") is None:
            return
        output.add_message(Message(message_id="visited", stream_id="visits"))
        if ctx.parameters.get("evict"):
            state.clear()


class EvictViaJoinerTester(RowFunction):
    """on_visit that tries to evict a read-only joined state — must raise."""

    def on_message(self, message, output, ctx):
        pass

    def on_visit(self, visit, output, ctx):
        ctx.joined_external_state(_WORD_JOINER, visit).clear()


class TestEvictTester:
    def _seed_and_visit(self, evict):
        h = ComputationHarness(
            EvictTester(),
            streams={"input": schema(x="string"), "visits": schema(x="string")},
            key_schema=schema(k="string"),
            external_states={_USER_STATE: schema(seen="int64")},
            parameters={"evict": evict},
        )
        key = h.build_key(k="user-1")
        msg = h.build_message("input", key=key, x="hello")
        visit = h.build_visit(key=key)

        seed = PayloadBuilder(schema(seen="int64")).set("seen", 1).finish()
        with h.processing([msg], visits=[visit], external_states={_USER_STATE: {key: seed}}) as r:
            return h, key, r

    def test_evict_true_resets_state(self):
        _, key, r = self._seed_and_visit(evict=True)

        assert any(m.message_id == "visited" for m in r.messages)
        assert r.external_state_is_reset(_USER_STATE, key) is True

    def test_evict_false_leaves_state(self):
        _, key, r = self._seed_and_visit(evict=False)

        assert any(m.message_id == "visited" for m in r.messages)
        assert r.external_state_is_reset(_USER_STATE, key) is False

    def test_joiner_cannot_evict(self):
        h = ComputationHarness(
            EvictViaJoinerTester(),
            streams={"input": schema(x="string")},
            key_schema=schema(word="string"),
            joined_external_states={_WORD_JOINER: schema(count="int64")},
        )
        key = h.build_key(word="hello")
        visit = h.build_visit(key=key)

        with pytest.raises(ReadOnlyExternalStateError):
            with h.processing(visits=[visit], joined_external_states={_WORD_JOINER: {}}):
                pass
