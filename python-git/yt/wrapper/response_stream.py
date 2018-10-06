from . import common

from yt.packages.six import Iterator

class ResponseStream(Iterator):
    """Iterator over response."""
    def __init__(self, get_response, iter_content, close, process_error, get_response_parameters):
        self._buffer = b""
        self._buffer_length = 0
        self._pos = 0

        self._get_response = get_response
        self._iter_content = iter_content
        self._close = close
        self._process_error = process_error

        self._is_closed = False
        self._stream_finished = False
        self._fetch()

        self.response_parameters = get_response_parameters()

    def chunk_iter(self):
        while True:
            result = self._read_chunk()
            if not result:
                break
            yield result

    def read(self, length=None):
        if self._stream_finished:
            return b""

        if length is None:
            length = 2 ** 32

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

    def readline(self):
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

    def _read_chunk(self):
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

    def __next__(self):
        line = self.readline()
        if not line:
            raise StopIteration()
        return line

    def close(self):
        if not self._is_closed:
            self._close()
            self._is_closed = True

class EmptyResponseStream(Iterator):
    def read(self, length=None):
        return b""

    def chunk_iter(self):
        return common.EMPTY_GENERATOR

    def readline(self):
        return b""

    def close(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration()

class ResponseStreamWithDel(ResponseStream):
    def __init__(self, *args, **kwargs):
        super(ResponseStreamWithDel, self).__init__(*args, **kwargs)

    def __del__(self):
        self.close()

class ResponseStreamWithReadRow(ResponseStreamWithDel):
    def __init__(self, *args, **kwargs):
        super(ResponseStreamWithReadRow, self).__init__(*args, **kwargs)

    def read_rows(self):
        iterator = self.chunk_iter()
        while True:
            assert self._pos == 0 or self._pos == self._buffer_length
            try:
                yield next(iterator)
            except StopIteration:
                return
