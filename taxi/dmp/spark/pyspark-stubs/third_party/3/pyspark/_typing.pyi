from typing import Any, Generic, Iterable, List, Optional, Sized, TypeVar, Union
from typing_extensions import Protocol

T = TypeVar("T", covariant=True)

PrimitiveType = Union[bool, float, int, str]

class SupportsIAdd(Protocol):
    def __iadd__(self, other: SupportsIAdd) -> SupportsIAdd: ...

class SupportsOrdering(Protocol):
    def __le__(self, other: SupportsOrdering) -> bool: ...

class SizedIterable(Protocol, Sized, Iterable[T]): ...
