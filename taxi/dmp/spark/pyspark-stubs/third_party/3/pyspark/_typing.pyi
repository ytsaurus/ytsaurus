from typing import Any, List, Optional, TypeVar, Union
from typing_extensions import Protocol


class SupportsIAdd(Protocol):
    def __iadd__(self, other: SupportsIAdd) -> SupportsIAdd: ...
