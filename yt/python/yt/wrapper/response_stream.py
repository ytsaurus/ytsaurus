from . import common

from collections.abc import Iterable
import typing

if typing.TYPE_CHECKING:
    import requests
else:
    from yt.packages import requests


class ResponseStream(Iterable):
    """Iterable over response."""
    def __init__(
            self,
            get_response: typing.Callable[[None], typing.Optional[requests.Response]],
            iter_content: typing.Iterator[bytes],
            close: typing.Callable[[bool], None],
            process_error: typing.Callable[[requests.Response], None],
            get_response_parameters: typing.Callable[[None], typing.Optional[typing.Dict[str, typing.Any]]],
    ):
        self._buffer = b""
        self._buffer_length = 0
        self._pos = 0

        self._get_response = get_response
        self._iter_content = iter_content
        self._close_actions = [close]
        self._process_error = process_error

        self._is_closed = False
        self._stream_finished = False
        self._fetch()

        self.response_parameters = get_response_parameters()

    def chunk_iter(self) -> typing.Generator[bytes, None, None]:
        """Yield data chunks from response"""
        while True:
            result = self._read_chunk()
            if not result:
                break
            yield result

    def read(self, length: int = None) -> bytes:
        """Read `length` bytes (or all) from current position"""
        if self._stream_finished:
            return b""

        if length is None:
            length = 2 ** 63

        result_strings = []
        if self._pos:
            if self._buffer_length <= self._pos + length:
                length -= self._buffer_length - self._pos
                string = self._buffer[self._pos:]
                self._fetch()
                if self._stream_finished:
                    return string
                else:
                    result_strings.append(string)
            else:
                pos = self._pos
                self._pos += length
                return self._buffer[pos:pos + length]

        while length > self._buffer_length:
            result_strings.append(self._buffer)
            length -= self._buffer_length
            self._fetch()
            if self._stream_finished:
                break

        if not self._stream_finished and length:
            result_strings.append(self._buffer[:length])
            self._pos = length

        return b"".join(result_strings)

    def readline(self) -> bytes:
        """Read bytes to the nearest new line"""
        if self._stream_finished:
            return b""

        result = []
        while True:
            index = self._buffer.find(b"\n", self._pos)
            if index != -1:
                result.append(self._buffer[self._pos: index + 1])
                self._pos = index + 1
                break

            result.append(self._buffer[self._pos:])
            self._fetch()
            if self._stream_finished:
                break

        return b"".join(result)

    def add_close_action(self, action: typing.Callable[[bool], None]):
        if self._is_closed:
            action(from_delete=False)
        else:
            self._close_actions.append(action)

    def _read_chunk(self) -> bytes:
        """Returns unreaded data from buffer, move cursor, fetch new data from response if nedded"""
        if self._pos == 0:
            remaining_buffer = self._buffer
        elif self._pos == len(self._buffer):
            self._fetch()
            if self._stream_finished:
                return b""
            remaining_buffer = self._buffer
        else:
            remaining_buffer = self._buffer[self._pos:]

        self._pos = len(self._buffer)

        return remaining_buffer

    def _fetch(self):
        """Fetch one data block into buffer from response"""
        assert not self._stream_finished
        try:
            while True:
                self._buffer = next(self._iter_content)
                if self._buffer:
                    break
            self._buffer_length = len(self._buffer)
            self._pos = 0
        except StopIteration:
            self._process_error(self._get_response())
            self._stream_finished = True
            self.close()

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        """Iterate over lines"""
        line = self.readline()
        if not line:
            raise StopIteration()
        return line

    def close(self, from_delete=False):
        if not self._is_closed:
            for action in self._close_actions:
                action(from_delete)
            self._is_closed = True

    @property
    def closed(self):
        return self._is_closed


class EmptyResponseStream(Iterable):
    def read(self, length=None):
        return b""

    def chunk_iter(self):
        return common.EMPTY_GENERATOR

    def readline(self):
        return b""

    def close(self):
        pass

    @property
    def closed(self):
        return True

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration()


class ResponseStreamWithDel(ResponseStream):
    def __init__(self, *args, **kwargs):
        super(ResponseStreamWithDel, self).__init__(*args, **kwargs)

    def __del__(self):
        self.close(from_delete=True)


class ResponseStreamWithReadRow(ResponseStreamWithDel):
    def __init__(self, *args, **kwargs):
        super(ResponseStreamWithReadRow, self).__init__(*args, **kwargs)

    def _read_rows(self):
        iterator = self.chunk_iter()
        while True:
            assert self._pos == 0 or self._pos == self._buffer_length
            try:
                yield next(iterator)
            except StopIteration:
                return
