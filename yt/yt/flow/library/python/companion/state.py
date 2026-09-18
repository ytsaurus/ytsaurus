"""State classes: State, ExternalState, StatesHolder."""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, Optional, TypeVar

from .row import Payload, TableSchema
from .wire_protocol import UnversionedRow


@dataclass
class State:
    """Wrapper for binary internal state.

    Next to the wire bytes the entry keeps the decoded value once an accessor hands it out, and
    the encoder that turns it back into bytes, so that the changes made to that value in place
    are picked up by :meth:`sync_bytes`.
    """

    reset: bool = False
    state: Optional[bytes] = None
    value: Any = field(default=None, compare=False, repr=False)
    codec: Any = field(default=None, compare=False, repr=False)
    encode: Optional[Callable[[Any], bytes]] = field(default=None, compare=False, repr=False)

    def get_value(self, codec: Any, decode: Callable[[bytes], Any]) -> Any:
        """Value decoded from the wire bytes, memoized per |codec|: every accessor of that codec
        gets the same object, while an accessor of another one decodes the bytes anew.

        Decoding anew takes the bytes as they arrived, so it drops a change made in place through
        the previous codec.
        """
        if self.value is None or self.codec != codec:
            self.value = decode(self.state)
            self.codec = codec
            self.encode = None
        return self.value

    def get_mutable_value(self, codec: Any, decode: Callable[[bytes], Any], encode: Callable[[Any], bytes]) -> Any:
        """Value like :meth:`get_value`, tracked for changes: |encode| is kept, so that
        :meth:`sync_bytes` re-encodes the value and the changes made to it in place reach the wire.
        """
        value = self.get_value(codec, decode)
        self.encode = encode
        return value

    def sync_bytes(self) -> bool:
        """Re-encode a tracked value and report whether the wire bytes changed."""
        if self.encode is None:
            return False
        encoded = self.encode(self.value)
        if encoded == self.state:
            return False
        self.state = encoded
        return True


@dataclass
class ExternalState:
    """Wrapper for external state backed by Payload."""

    reset: bool = False
    state: Optional[Payload] = None

    def sync_bytes(self) -> bool:
        """External state is written through the accessor's ``set()``, never tracked in place."""
        return False


# Sentinel for reset external state.
EXTERNAL_STATE_RESET = ExternalState(reset=True, state=None)


T = TypeVar("T", State, ExternalState)


class StatesHolder(Generic[T]):
    """Holder of raw binary state representation, keyed by UnversionedRow."""

    def __init__(
        self,
        name: str,
        key_schema: Optional[TableSchema] = None,
        state_schema: Optional[TableSchema] = None,
    ):
        self.name = name
        self.key_schema = key_schema
        self.state_schema = state_schema
        self._states: Dict[tuple, tuple[UnversionedRow, T]] = {}
        # Entries changed during the current epoch: via set() (writes from state accessors), or
        # in place through a mutable value, as found by collect_modified().
        # Keys populated from the incoming request via load() are intentionally excluded so that
        # only modified states are sent back in the response.  The entry is kept, not just its
        # key: a state attached over a pending reset must not turn that reset into a write.
        self._modified: Dict[tuple, tuple[UnversionedRow, T]] = {}

    def _row_key(self, row: UnversionedRow) -> tuple:
        """Create a hashable key from an UnversionedRow."""
        parts = []
        for v in row.values:
            parts.append((v.column_id, v.type, v.value if not isinstance(v.value, bytearray) else bytes(v.value)))
        return tuple(parts)

    def set(self, key: UnversionedRow, value: T):
        """Set a value for key and mark it modified (so it is sent back in the response)."""
        row_key = self._row_key(key)
        self._states[row_key] = (key, value)
        self._modified[row_key] = (key, value)

    def load(self, key: UnversionedRow, value: T):
        """Load a value from the request WITHOUT marking it modified.

        Used to populate the holder from the incoming request: such states must not be echoed
        back unless a state accessor changes them.
        """
        self._states[self._row_key(key)] = (key, value)

    def get(self, key: UnversionedRow) -> Optional[T]:
        entry = self._states.get(self._row_key(key))
        return entry[1] if entry else None

    def items(self):
        """Iterate over (key, value) pairs for all states."""
        for row, val in self._states.values():
            yield row, val

    def modified_items(self):
        """Iterate over (key, value) pairs for states modified this epoch."""
        for row, value in self._modified.values():
            yield row, value

    def collect_modified(self) -> bool:
        """Re-encode the values handed out as mutable and report whether the holder has anything
        to send back: a value changed in place counts as modified, one that re-encodes to the
        bytes it arrived with does not.
        """
        for row_key, entry in self._states.items():
            if entry[1].sync_bytes():
                self._modified[row_key] = entry
        return self.has_modified()

    def has_modified(self) -> bool:
        """Whether any state was modified during the current epoch; call collect_modified() first
        to pick up the values changed in place.
        """
        return bool(self._modified)
