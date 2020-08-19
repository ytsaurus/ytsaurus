from .errors import YtError

import struct


class UnframingStream(object):
    DATA_TAG = 0x1
    KEEP_ALIVE_TAG = 0x2

    def __init__(self, underlying):
        """
        :param urllib3.HTTPResponse underlying: stream to wrap.
        """
        self._underlying = underlying
        self._remaining_size = 0
        self._buffer = b""
        self._buffer_offset = 0

    def iter_content(self, size):
        while True:
            chunk = self.read(size)
            if len(chunk) == 0:
                break
            yield chunk

    def read(self, size):
        if self._remaining_size == 0:
            self._refresh_frame()
        size = min(size, self._remaining_size)
        chunk = self._read_impl(size)
        self._remaining_size -= len(chunk)
        if len(chunk) == 0 and size > 0 and self._remaining_size > 0:
            raise YtError(
                "Unexpected end of framing stream: incomplete data frame (expected {} more bytes)"
                .format(self._remaining_size)
            )
        return chunk

    def close(self):
        self._underlying.close()

    def _refresh_frame(self):
        assert self._remaining_size == 0
        while True:
            tag_buffer = self._read_impl(1)
            if len(tag_buffer) == 0:
                return
            tag = ord(tag_buffer)
            if tag == self.DATA_TAG:
                size_buffer = self._read_full(4)
                if len(size_buffer) < 4:
                    raise YtError("Unexpected end of framing stream: incomplete data frame size")
                (self._remaining_size,) = struct.unpack("<i", size_buffer)
                return
            elif tag == self.KEEP_ALIVE_TAG:
                continue
            else:
                raise YtError("Incorrect frame tag {}".format(tag))

    def _read_full(self, size):
        """ Reads either size bytes or all remaining, whichever is smaller """
        chunks = []
        while size > 0:
            chunk = self._read_impl(size)
            if len(chunk) == 0:
                break
            size -= len(chunk)
            chunks.append(chunk)
        return b"".join(chunks)

    def _read_impl(self, size):
        assert size is not None
        assert self._buffer_offset <= len(self._buffer)
        if self._buffer_offset == len(self._buffer):
            self._buffer_offset = 0
            self._buffer = b""
            while not self._underlying.isclosed():
                self._buffer = self._underlying.read(size, decode_content=True)
                if len(self._buffer) != 0:
                    break
        size = min(size, len(self._buffer) - self._buffer_offset)
        chunk = self._buffer[self._buffer_offset: self._buffer_offset + size]
        self._buffer_offset += size
        return chunk
