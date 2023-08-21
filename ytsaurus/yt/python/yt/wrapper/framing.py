from .errors import YtError

import yt.logger as logger

from yt.packages.requests.utils import stream_decode_response_unicode

import struct

DATA_TAG = 0x1
KEEP_ALIVE_TAG = 0x2


class UnframingStream(object):
    def __init__(self, response, chunk_size):
        """
        :param requests.Response response: stream to wrap.
        :param chunk_size: desired chunk size.
        """
        self._framed_chunks = response.framed_iter_content(chunk_size=chunk_size, decode_unicode=False)
        self._buffer = b""
        self._buffer_offset = 0

    def read(self, size):
        assert isinstance(size, int)
        assert self._buffer_offset <= len(self._buffer)
        if self._buffer_offset == len(self._buffer):
            self._buffer_offset = 0
            self._buffer = b""
            try:
                self._buffer = next(self._framed_chunks)
            except StopIteration:
                pass
        size = min(size, len(self._buffer) - self._buffer_offset)
        chunk = self._buffer[self._buffer_offset: self._buffer_offset + size]
        self._buffer_offset += size
        return chunk

    def read_full(self, size):
        """ Reads either size bytes or all remaining, whichever is smaller """
        assert isinstance(size, int)
        assert self._buffer_offset <= len(self._buffer)
        chunks = []
        while size > 0:
            chunk = self.read(size)
            if len(chunk) == 0:
                break
            size -= len(chunk)
            chunks.append(chunk)
        return b"".join(chunks)


def unframed_iter_content(response, chunk_size=1, decode_unicode=False):
    unframing_stream = UnframingStream(response, chunk_size=chunk_size)

    def generator():
        while True:
            tag_buffer = unframing_stream.read(1)
            if len(tag_buffer) == 0:
                return
            tag = ord(tag_buffer)
            if tag == DATA_TAG:
                size_buffer = unframing_stream.read_full(4)
                if len(size_buffer) < 4:
                    message = "Unexpected end of framing stream: " \
                        "incomplete data frame size: expected 4 bytes, got {}" \
                        .format(len(size_buffer))
                    logger.error(message)
                    response.framing_error = YtError(message)
                    return
                (remaining_size,) = struct.unpack("<i", size_buffer)
                while remaining_size > 0:
                    size = min(chunk_size, remaining_size) if chunk_size is not None else remaining_size
                    chunk = unframing_stream.read(size)
                    if len(chunk) == 0:
                        message = "Unexpected end of framing stream: " \
                            "incomplete data frame: expected {} more bytes" \
                            .format(remaining_size)
                        logger.error(message)
                        response.framing_error = YtError(message)
                        return
                    yield chunk
                    remaining_size -= len(chunk)
            elif tag == KEEP_ALIVE_TAG:
                continue
            else:
                raise YtError("Incorrect frame tag {}".format(tag))

    chunks = generator()
    if decode_unicode:
        chunks = stream_decode_response_unicode(chunks, response)
    return chunks
