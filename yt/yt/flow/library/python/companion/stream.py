"""Stream classes: FlowStream, RawStream, StreamIdsMapping, StreamSpecs, FlowStreamsContext."""

from typing import Any, Dict, Optional

from .row import Payload, TableSchema
from .wire_protocol import WireProtocolReader, WireProtocolWriter


class FlowStream:
    """Base class representing a data stream with unique ID and schema."""

    def __init__(self, stream_id: str, schema: Optional[TableSchema] = None):
        self._stream_id = stream_id
        self._schema = schema

    @property
    def stream_id(self) -> str:
        return self._stream_id

    @property
    def schema(self) -> Optional[TableSchema]:
        return self._schema

    def from_proto(self, data: bytes) -> Payload:
        """Deserialize bytes into a Payload."""
        raise NotImplementedError

    def to_proto(self, payload: Any) -> bytes:
        """Serialize a Payload into bytes."""
        raise NotImplementedError


class RawStream(FlowStream):
    """Default FlowStream implementation using wire protocol for serialization."""

    def __init__(self, stream_id: str, schema: Optional[TableSchema] = None):
        super().__init__(stream_id, schema)

    def from_proto(self, data: bytes) -> Payload:
        reader = WireProtocolReader(data)
        row = reader.read_unversioned_row()
        if row is None:
            return Payload.EMPTY
        return Payload(row, self._schema)

    def to_proto(self, payload: Any) -> bytes:
        if isinstance(payload, Payload):
            writer = WireProtocolWriter()
            return writer.write_unversioned_row(payload.row)
        raise TypeError(f"Expected Payload, got {type(payload)}")


class StreamIdsMapping:
    """Bidirectional mapping between stream_id (str) and stream_spec_id (int)."""

    def __init__(self, mapping: Optional[Dict[str, int]] = None):
        self._id_to_spec: Dict[str, int] = {}
        self._spec_to_id: Dict[int, str] = {}
        if mapping:
            for stream_id, spec_id in mapping.items():
                self.add_mapping(stream_id, spec_id)

    def add_mapping(self, stream_id: str, stream_spec_id: int):
        self._id_to_spec[stream_id] = stream_spec_id
        self._spec_to_id[stream_spec_id] = stream_id

    def get_stream_id(self, spec_id: int) -> Optional[str]:
        return self._spec_to_id.get(spec_id)

    def get_stream_spec_id(self, stream_id: str) -> Optional[int]:
        return self._id_to_spec.get(stream_id)


class StreamSpecs:
    """Combines StreamIdsMapping with a list of FlowStream instances."""

    def __init__(self, mapping: StreamIdsMapping, streams: list[FlowStream]):
        self._mapping = mapping
        self._stream_by_id: Dict[str, FlowStream] = {s.stream_id: s for s in streams}

    def get_stream_id(self, spec_id: int) -> Optional[str]:
        return self._mapping.get_stream_id(spec_id)

    def get_stream_spec_id(self, stream_id: str) -> Optional[int]:
        return self._mapping.get_stream_spec_id(stream_id)

    def get_stream(self, stream_id: str) -> Optional[FlowStream]:
        return self._stream_by_id.get(stream_id)


class FlowStreamsContext:
    """Registry of FlowStream instances by stream_id."""

    def __init__(self):
        self._streams: Dict[str, FlowStream] = {}

    def add_stream(self, stream_id: str, stream: FlowStream):
        self._streams[stream_id] = stream

    def get_stream(self, stream_id: str) -> Optional[FlowStream]:
        return self._streams.get(stream_id)
