import common

class ResponseStream(object):
    """Iterator over response"""
    def __init__(self, get_response, iter_content, close, process_error, get_response_parameters):
        self._buffer = ""
        self._buffer_length = 0
        self._pos = 0

        self._get_response = get_response
        self._iter_content = iter_content
        self._close = close
        self._process_error = process_error

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
            return ""

        if length is None:
            length = 2 ** 32

        result_strings = []

        assert self._buffer_length

        buffer_consumed = False
        if self._pos:
            right = self._pos + length
            if self._buffer_length <= right:
                right = self._buffer_length
                buffer_consumed = True
            result_strings.append(self._buffer[self._pos:right])
            length -= right - self._pos

            if buffer_consumed:
                self._fetch()
                if self._stream_finished:
                    return result_strings[0]
            else:
                self._pos = right

        while length > self._buffer_length:
            result_strings.append(self._buffer)
            length -= self._buffer_length
            self._fetch()
            if self._stream_finished:
                break

        if not self._stream_finished:
            result_strings.append(self._buffer[:length])
            self._pos = length

        return "".join(result_strings)

    def readline(self):
        if self._stream_finished:
            return ""

        result = []
        while True:
            index = self._buffer.find("\n", self._pos)
            if index != -1:
                result.append(self._buffer[self._pos: index + 1])
                self._pos = index + 1
                break

            result.append(self._buffer[self._pos:])
            self._fetch()
            if self._stream_finished:
                break

        return "".join(result)

    def _read_chunk(self):
        if self._pos == 0:
            remaining_buffer = self._buffer
        elif self._pos == len(self._buffer):
            self._fetch()
            if self._stream_finished:
                return ""
            remaining_buffer = self._buffer
        else:
            remaining_buffer = self._buffer[self._pos:]

        self._pos = len(self._buffer)

        return remaining_buffer

    def _fetch(self):
        assert not self._stream_finished
        try:
            self._buffer = self._iter_content.next()
            self._buffer_length = len(self._buffer)
            self._pos = 0
            if not self._buffer_length:
                self._process_error(self._get_response())
                self._stream_finished = True
        except StopIteration:
            self._process_error(self._get_response())
            self._stream_finished = True

    def __iter__(self):
        return self

    def next(self):
        line = self.readline()
        if not line:
            raise StopIteration()
        return line

    def close(self):
        self._close()

class EmptyResponseStream(object):
    def read(self, length=None):
        return ""

    def chunk_iter(self):
        return common.EMPTY_GENERATOR

    def readline(self):
        return ""

    def close(self):
        pass

    def __iter__(self):
        return self

    def next(self):
        raise StopIteration()

