"""Tests for the Pythonic Pipeline API."""

import pytest

from yt.yt.flow.library.python.companion._api import Pipeline
from yt.yt.flow.library.python.companion.computation import (
    RowFunction,
    SourceComputation,
)
from yt.yt.flow.library.python.companion.row import (
    ColumnSchema,
    Message,
    Payload,
    TableSchema,
)
from yt.yt.flow.library.python.companion.stream import (
    StreamIdsMapping,
)
import yt.type_info as ti

from yt.yt.flow.library.python.companion.wire_protocol import (
    ColumnValueType,
    UnversionedRow,
    UnversionedValue,
)


class TestPayloadDictAccess:
    def _make_payload(self):
        schema = TableSchema(
            [
                ColumnSchema("word", ti.String),
                ColumnSchema("count", ti.Int64),
            ]
        )
        row = UnversionedRow(
            values=[
                UnversionedValue(column_id=0, type=ColumnValueType.STRING, value=b"hello"),
                UnversionedValue(column_id=1, type=ColumnValueType.INT64, value=42),
            ]
        )
        return Payload(row, schema)

    def test_getitem(self):
        payload = self._make_payload()
        assert payload["word"] == "hello"
        assert payload["count"] == 42

    def test_getitem_missing_raises(self):
        payload = self._make_payload()
        with pytest.raises(KeyError):
            payload["nonexistent"]

    def test_contains(self):
        payload = self._make_payload()
        assert "word" in payload
        assert "count" in payload
        assert "nonexistent" not in payload

    def test_keys(self):
        payload = self._make_payload()
        assert set(payload.keys()) == {"word", "count"}

    def test_to_dict(self):
        payload = self._make_payload()
        d = payload.to_dict()
        assert d["word"] == "hello"
        assert d["count"] == 42

    def test_empty_payload(self):
        payload = Payload.EMPTY
        assert payload.keys() == []
        assert payload.to_dict() == {}
        assert "anything" not in payload


class TestStreamIdsMappingDict:
    def test_dict_constructor(self):
        mapping = StreamIdsMapping({"stream_a": 0, "stream_b": 1})
        assert mapping.get_stream_id(0) == "stream_a"
        assert mapping.get_stream_id(1) == "stream_b"
        assert mapping.get_stream_spec_id("stream_a") == 0
        assert mapping.get_stream_spec_id("stream_b") == 1

    def test_empty_constructor(self):
        mapping = StreamIdsMapping()
        assert mapping.get_stream_id(0) is None


class TestPipelineRegistration:
    def test_decorator_plain_function(self):
        pipeline = Pipeline()

        @pipeline.computation("my-comp")
        def my_fn(message, output, ctx):
            pass

        comp = pipeline.context.get_computation("my-comp")
        assert comp is not None
        assert comp.computation_id == "my-comp"
        assert comp.function_type == "Row"

    def test_decorator_batch_function(self):
        pipeline = Pipeline()

        @pipeline.computation("batch-comp")
        def my_fn(messages, output, ctx):
            pass

        comp = pipeline.context.get_computation("batch-comp")
        assert comp is not None
        assert comp.function_type == "Batch"

    def test_decorator_class_based(self):
        pipeline = Pipeline()

        class MyFunc(RowFunction):
            def on_message(self, message, output, ctx):
                pass

        pipeline.add("class-comp", MyFunc())
        comp = pipeline.context.get_computation("class-comp")
        assert comp is not None
        assert comp.function_type == "Row"

    def test_decorator_source_computation(self):
        pipeline = Pipeline()

        @pipeline.computation("src-comp", source=True)
        def my_fn(message, output, ctx):
            pass

        comp = pipeline.context.get_computation("src-comp")
        assert comp is not None
        assert isinstance(comp, SourceComputation)
        assert comp.is_source

    def test_imperative_add(self):
        pipeline = Pipeline()

        def my_fn(message, output, ctx):
            output.add_message(Message(message_id=message.message_id, stream_id="out"))

        pipeline.add("imp-comp", my_fn)
        comp = pipeline.context.get_computation("imp-comp")
        assert comp is not None
        assert comp.function_type == "Row"
