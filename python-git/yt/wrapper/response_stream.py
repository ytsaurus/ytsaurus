import common

class ResponseStream(object):
    """Iterator over response"""
    def __init__(self, get_response, iter_content, close, process_error, get_response_parameters):
        self._buffer = ""
        self._pos = 0

        self._get_response = get_response
        self._iter_content = iter_content
        self._close = close
        self._process_error = process_error

        self._fetch()
        self.response_parameters = get_response_parameters()

    def chunk_iter(self):
        while True:
            result = self._read_chunk()
            if not result:
                break
            yield result

    def read(self, length=None):
        if length is None:
            length = 2 ** 32

        result = []
        if not self._buffer:
            self._fetch()

        while length > 0:
            right = self._pos + length
            if len(self._buffer) < right:
                right = len(self._buffer)
            result.append(self._buffer[self._pos:right])
            length -= right - self._pos
            self._pos = right
            if length == 0 or not self._fetch():
                break
        return "".join(result)

    def readline(self):
        result = []
        while True:
            index = self._buffer.find("\n", self._pos)
            if index != -1:
                result.append(self._buffer[self._pos: index + 1])
                self._pos = index + 1
                break

            result.append(self._buffer[self._pos:])
            if not self._fetch():
                self._buffer = ""
                self._pos = 0
                break
        return "".join(result)

    def _read_chunk(self):
        if not self._buffer and not self._fetch():
            return ""

        if self._pos == 0:
            remaining_buffer = self._buffer
        elif self._pos == len(self._buffer):
            self._fetch()
            remaining_buffer = self._buffer
        else:
            remaining_buffer = self._buffer[self._pos:]

        self._buffer = ""
        self._pos = 0
        return remaining_buffer

    def _fetch(self):
        try:
            self._buffer = self._iter_content.next()
            self._pos = 0
            if not self._buffer:
                self._process_error(self._get_response())
                return False
            return True
        except StopIteration:
            self._process_error(self._get_response())
            return False

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

