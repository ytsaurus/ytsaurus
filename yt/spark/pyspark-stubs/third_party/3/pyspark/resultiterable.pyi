# Stubs for pyspark.resultiterable (Python 3.5)
#

from pyspark._typing import SizedIterable
from typing import Generic, Iterable, Iterator, TypeVar

T = TypeVar("T")

class ResultIterable(SizedIterable[T]):
    data: SizedIterable[T]
    index: int
    maxindex: int
    def __init__(self, data: SizedIterable[T]) -> None: ...
    def __iter__(self) -> Iterator[T]: ...
    def __len__(self) -> int: ...
