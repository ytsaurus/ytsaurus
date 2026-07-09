"""Response wrappers for test assertions."""

from typing import Any, Dict, List, Optional

from ..context import ResponseContext
from ..row import Message, NewTimer, Payload
from ..state import ExternalState, State, StatesHolder


class ProcessResponse:
    """Wraps a ResponseContext with convenient assertion-friendly accessors."""

    def __init__(self, response_ctx: ResponseContext, joined_external_states: Optional[Dict[str, StatesHolder]] = None):
        self._ctx = response_ctx
        self._joined_external_states = joined_external_states or {}

    @property
    def messages(self) -> List[Message]:
        result = []
        for tr in self._ctx.transform_results:
            result.extend(tr.messages)
        return result

    @property
    def distribute(self) -> List[bool]:
        result = []
        for tr in self._ctx.transform_results:
            result.extend(tr.distribute)
        return result

    @property
    def timers(self) -> List[NewTimer]:
        result = []
        for tr in self._ctx.transform_results:
            result.extend(tr.timers)
        return result

    # -- Internal state (YSON) --

    def internal_state(self, name: str, key: Payload) -> Any:
        """Get internal state as a YSON-deserialized Python object."""
        import yt.yson as yson

        raw = self.internal_state_raw(name, key)
        if raw is None or raw.reset or raw.state is None:
            return None
        return yson.loads(raw.state)

    def internal_state_proto(self, name: str, key: Payload, proto_class):
        """Get internal state deserialized as a protobuf message."""
        raw = self.internal_state_raw(name, key)
        if raw is None or raw.reset or raw.state is None:
            return None
        msg = proto_class()
        msg.ParseFromString(raw.state)
        return msg

    def internal_state_raw(self, name: str, key: Payload) -> Optional[State]:
        """Get the raw State object (for carrying to next processing step)."""
        holder = self._ctx.internal_states.get(name)
        if holder is None:
            return None
        return holder.get(key.row)

    def internal_state_size(self, name: str) -> int:
        holder = self._ctx.internal_states.get(name)
        if holder is None:
            return 0
        return sum(1 for _ in holder.items())

    def internal_state_is_reset(self, name: str, key: Payload) -> bool:
        raw = self.internal_state_raw(name, key)
        return raw is not None and raw.reset

    # -- External state --

    def external_state(self, name: str, key: Payload) -> Optional[Payload]:
        """Get external state as a Payload."""
        raw = self.external_state_raw(name, key)
        if raw is None or raw.reset or raw.state is None:
            return None
        return raw.state

    def external_state_raw(self, name: str, key: Payload) -> Optional[ExternalState]:
        """Get the raw ExternalState object (for carrying to next processing step)."""
        holder = self._ctx.external_states.get(name)
        if holder is None:
            return None
        return holder.get(key.row)

    def external_state_size(self, name: str) -> int:
        holder = self._ctx.external_states.get(name)
        if holder is None:
            return 0
        return sum(1 for _ in holder.items())

    def external_state_is_reset(self, name: str, key: Payload) -> bool:
        raw = self.external_state_raw(name, key)
        return raw is not None and raw.reset

    def has_external_state(self, name: str) -> bool:
        """Whether the response carries a modified external state holder for name."""
        holder = self._ctx.external_states.get(name)
        return holder is not None and holder.has_modified()

    # -- Joined (read-only) external state --
    #
    # Joined external state has no TResponseData counterpart, so it is not part of the
    # ResponseContext.  These accessors read the request-side joined holders to let tests
    # assert that joiners observed the writer value and never wrote back.

    def joined_external_state(self, name: str, key: Payload) -> Optional[Payload]:
        """Get joined external state as a Payload."""
        raw = self.joined_external_state_raw(name, key)
        if raw is None or raw.reset or raw.state is None:
            return None
        return raw.state

    def joined_external_state_raw(self, name: str, key: Payload) -> Optional[ExternalState]:
        holder = self._joined_external_states.get(name)
        if holder is None:
            return None
        return holder.get(key.row)

    def joined_external_state_holder(self, name: str) -> Optional[StatesHolder]:
        """Access the joined external state holder (for has_modified assertions)."""
        return self._joined_external_states.get(name)
