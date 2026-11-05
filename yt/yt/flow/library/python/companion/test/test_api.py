"""Tests for the Pythonic Pipeline API."""

import pytest
from google.protobuf.descriptor_pb2 import FileDescriptorProto, FileDescriptorSet
from google.protobuf.message import Message as ProtoMessage

from yt.yt.flow.library.python.companion._api import Pipeline
from yt.yt.flow.library.python.companion.computation import (
    RowFunction,
    SourceComputation,
)
from yt.yt.flow.library.python.companion.resource import FlowResource
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


class _PlainResource(FlowResource):
    pass


class _NamedResource(FlowResource):
    flow_resource_class = "named_resource"


class _NamedResourceChild(_NamedResource):
    pass


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

    def test_add_resource_by_class_derives_qualified_name(self):
        pipeline = Pipeline()
        pipeline.add_resource(_PlainResource)

        factories = pipeline.context.get_resource_factories()
        assert factories[f"{__name__}.{_PlainResource.__qualname__}"] is _PlainResource

    def test_add_resource_by_class_honors_the_name_attribute(self):
        pipeline = Pipeline()
        pipeline.add_resource(_NamedResource)

        assert pipeline.context.get_resource_factories()["named_resource"] is _NamedResource

    def test_add_resource_name_attribute_is_not_inherited(self):
        pipeline = Pipeline()
        pipeline.add_resource(_NamedResourceChild)

        factories = pipeline.context.get_resource_factories()
        assert factories[f"{__name__}.{_NamedResourceChild.__qualname__}"] is _NamedResourceChild

    def test_add_resource_by_class_takes_an_explicit_factory(self):
        pipeline = Pipeline()
        factory = _PlainResource
        pipeline.add_resource(_NamedResource, factory)

        assert pipeline.context.get_resource_factories()["named_resource"] is factory

    def test_add_resource_by_name_requires_a_factory(self):
        pipeline = Pipeline()
        with pytest.raises(TypeError):
            pipeline.add_resource("resource_class")

    def test_add_resource_rejects_a_non_resource_class(self):
        pipeline = Pipeline()
        with pytest.raises(TypeError):
            pipeline.add_resource(object)

    def test_add_resource_rejects_a_duplicate_derived_name(self):
        pipeline = Pipeline()
        pipeline.add_resource(_PlainResource)
        with pytest.raises(ValueError):
            pipeline.add_resource(_PlainResource)


class TestProtoStateDeclarations:
    @staticmethod
    def noop(message, output, ctx):
        pass

    def test_add_and_decorator_scope_states_and_copy_declarations(self):
        pipeline = Pipeline()
        declarations = {"/state": FileDescriptorProto}
        pipeline.add("owner", self.noop, proto_external_states=declarations)
        declarations["/state"] = FileDescriptorSet

        @pipeline.computation("reader", joined_proto_external_states={"/state": FileDescriptorSet})
        def reader(message, output, ctx):
            pass

        assert pipeline.context.get_proto_external_states() == {"owner": {"/state": FileDescriptorProto}}
        assert pipeline.context.get_joined_proto_external_states() == {"reader": {"/state": FileDescriptorSet}}
        snapshot = pipeline.context.get_proto_external_states()
        snapshot["owner"]["/state"] = FileDescriptorSet
        assert pipeline.context.get_proto_external_states()["owner"]["/state"] is FileDescriptorProto

    @pytest.mark.parametrize(
        "invalid,valid",
        [
            ("proto_external_states", "joined_proto_external_states"),
            ("joined_proto_external_states", "proto_external_states"),
        ],
    )
    @pytest.mark.parametrize("proto_class", [object, FileDescriptorProto(), ProtoMessage])
    def test_invalid_class_leaves_registration_unchanged(self, invalid, valid, proto_class):
        pipeline = Pipeline()
        with pytest.raises(TypeError, match="requires a protobuf message class"):
            pipeline.add(
                "join", self.noop, **{valid: {"/valid": FileDescriptorProto}, invalid: {"/state": proto_class}}
            )
        assert pipeline.context.get_computation("join") is None
        assert pipeline.context.get_proto_external_states() == {}
        assert pipeline.context.get_joined_proto_external_states() == {}
        pipeline.add("join", self.noop, **{invalid: {"/state": FileDescriptorProto}})

    @pytest.mark.parametrize("name", ["", "state", "/", "/state/", "/a//b"])
    def test_invalid_state_name(self, name):
        pipeline = Pipeline()
        with pytest.raises(ValueError):
            pipeline.add("join", self.noop, proto_external_states={name: FileDescriptorProto})
        assert pipeline.context.get_computation("join") is None

    def test_frozen_context_rejects_declarations(self):
        pipeline = Pipeline()
        pipeline.context._freeze()
        with pytest.raises(RuntimeError, match="frozen"):
            pipeline.add("join", self.noop, proto_external_states={"/state": FileDescriptorProto})
        assert pipeline.context.get_proto_external_states() == {}

    def test_run_passes_declarations_to_runner(self, monkeypatch):
        from yt.yt.flow.library.python import runner

        monkeypatch.delenv("YT_FLOW_COMPANION_CONFIG", raising=False)
        monkeypatch.setattr(runner.sys, "argv", ["pipeline", "--config", "pipeline.yson", "--flow-bin", "/flow_server"])
        calls = []
        monkeypatch.setattr(runner, "launch", lambda *args, **kwargs: calls.append((args, kwargs)))
        pipeline = Pipeline()
        pipeline.add(
            "join",
            self.noop,
            proto_external_states={"/state": FileDescriptorProto},
            joined_proto_external_states={"/joined": FileDescriptorSet},
        )
        pipeline.run()
        assert calls == [
            (
                ("pipeline.yson", "/flow_server"),
                {
                    "proto_external_states": {"join": {"/state": FileDescriptorProto}},
                    "joined_proto_external_states": {"join": {"/joined": FileDescriptorSet}},
                },
            )
        ]

    def test_companion_mode_does_not_launch_runner(self, monkeypatch):
        from yt.yt.flow.library.python.companion import _api

        monkeypatch.setenv("YT_FLOW_COMPANION_CONFIG", "{}")
        executions = []

        class Execution:
            def __init__(self, context, port):
                self.context = context
                assert port == 1234

            def start(self):
                executions.append(self.context)

        monkeypatch.setattr(_api, "GrpcServerExecution", Execution)
        pipeline = Pipeline()
        pipeline.add("join", self.noop, proto_external_states={"/state": FileDescriptorProto})
        pipeline.run(port=1234)
        assert executions == [pipeline.context]
