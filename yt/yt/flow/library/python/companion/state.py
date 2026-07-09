"""State classes: State, ExternalState, StatesHolder."""

from dataclasses import dataclass
from typing import Dict, Generic, Optional, TypeVar

from .row import Payload, TableSchema
from .wire_protocol import UnversionedRow


@dataclass
class State:
    """Wrapper for binary internal state."""

    reset: bool = False
    state: Optional[bytes] = None


# Sentinel for reset state.
STATE_RESET = State(reset=True, state=None)


@dataclass
class ExternalState:
    """Wrapper for external state backed by Payload."""

    reset: bool = False
    state: Optional[Payload] = None


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
        # Row keys mutated via set() (writes from state accessors) during the current epoch.
        # Keys populated from the incoming request via load() are intentionally excluded so that
        # only modified states are sent back in the response.
        self._modified: set = set()

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
        self._modified.add(row_key)

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
        """Iterate over (key, value) pairs for states modified via set() this epoch."""
        for row_key in self._modified:
            entry = self._states.get(row_key)
            if entry is not None:
                yield entry[0], entry[1]

    def has_modified(self) -> bool:
        """Whether any state was modified via set() during the current epoch."""
        return bool(self._modified)
