from collections.abc import Iterable, Iterator


class Limiter[T]:
    def __init__(self, iterable: Iterable[T], limit: int) -> None:
        assert limit > 0
        self._len = len(iterable) if hasattr(iterable, "__len__") else None
        self._iterator = iter(iterable)
        self._limit = limit
        self._sent = 0
        self._rest = None

    def __iter__(self) -> Iterator[T]:
        return self

    def __next__(self) -> T:
        if self._sent == self._limit:
            raise StopIteration()
        val = next(self._iterator)
        self._sent += 1
        return val

    def rest_count(self) -> int:
        if self._len is not None:
            return self._len - self._sent
        if self._rest is None:
            self._rest = sum(1 for _ in self._iterator)
        return self._rest

    def total(self) -> int:
        return self._sent + self.rest_count()
