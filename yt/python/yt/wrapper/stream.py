from .common import get_stream_size_or_none, YtError, chunk_iter_stream

import os
import types


def _is_freshly_opened_file(stream):
    try:
        return hasattr(stream, "fileno") and stream.tell() == 0
    except IOError:
        return False


def _get_file_size(fstream):
    # We presuppose that current position in file is 0
    fstream.seek(0, os.SEEK_END)
    size = fstream.tell()
    fstream.seek(0, os.SEEK_SET)
    return size


def _stream_or_empty_bytes(stream):
    has_some = False
    for line in stream:
        has_some = True
        yield line
    if not has_some:
        yield b""


def _flatten_stream(stream):
    for chunk in stream:
        if isinstance(chunk, (list, tuple, types.GeneratorType)):
            for subchunk in _flatten_stream(chunk):
                yield subchunk
        else:
            yield chunk


def _split_chunks_by_max_size(stream, max_size):
    for chunk in stream:
        if len(chunk) <= max_size:
            yield [chunk]
        else:
            pieces = [chunk[start_index:start_index + max_size]
                      for start_index in range(0, len(chunk), max_size)]
            yield pieces


def _resplit_chunks(chunks, chunk_size):
    group = []
    length = 0
    for chunk in chunks:
        assert isinstance(chunk, (str, bytes))
        need_len = chunk_size - length
        if need_len >= len(chunk):
            head = chunk
            tail = None
        else:
            head = chunk[:need_len]
            tail = chunk[need_len:]
        group.append(head)
        length += len(group[-1])
        if length >= chunk_size:
            yield b"".join(group)
            group = []
            length = 0
            if tail:
                group.append(tail)
                length += len(tail)
    if group:
        yield b"".join(group)


def _merge_items_into_chunks(items, chunk_size):
    length = 0
    chunk_items = []
    for item in items:
        assert isinstance(item, (str, bytes))
        chunk_items.append(item)
        length += len(chunk_items[-1])
        if length >= chunk_size:
            yield b"".join(chunk_items)
            length = 0
            chunk_items = []
    if chunk_items:
        yield b"".join(chunk_items)


def _stream_isatty(stream):
    if hasattr(stream, "isatty"):
        return stream.isatty()
    else:
        return False


class Stream(object):
    """
        Base Stream class. A subclass class must set self._iter attribute.
    """

    def __init__(self, input, isatty=None):
        self._iter = iter(input)
        if isatty is None:
            self._isatty = _stream_isatty(input)
        else:
            self._isatty = isatty

    def _check_item_type(self, item):
        if not isinstance(item, bytes):
            raise TypeError("Stream expected to contain binary data but contains: '{}'.".format(
                type(item)
            ))

    def __iter__(self):
        return self

    def next(self):
        # Python 2 compatibility method.
        return self.__next__()

    def __next__(self):
        item = next(self._iter)
        self._check_item_type(item)
        return item

    def isatty(self):
        return self._isatty


class RawStream(Stream):
    """
        Represents a stream of raw homogeneous data.
    """

    def __init__(self, input, chunk_size):
        self.size = None
        isatty = _stream_isatty(input)

        if _is_freshly_opened_file(input):
            self.size = _get_file_size(input)
            # Read out file into the memory if it is small.
            if self.size <= chunk_size:
                input = input.read()
        else:
            self.size = get_stream_size_or_none(input)

        try:
            # NB: os.readlink is not defined on Windows.
            self.filename_hint = os.readlink("/proc/self/fd/0")
        except (IOError, OSError, AttributeError):
            self.filename_hint = None

        # Read stream by chunks. Also it helps to correctly process StringIO from cStringIO
        # (it has bug with default iteration). Also it allows to avoid reading file
        # by lines that may be slow.
        if hasattr(input, "read"):
            # read files by chunks, not by lines
            input = chunk_iter_stream(input, chunk_size)
        if isinstance(input, (str, bytes)):
            if isinstance(input, str):
                raise YtError("Only binary strings are supported as string input")

            self.size = len(input)
            input = [input]

        super(RawStream, self).__init__(input, isatty)

    def into_chunks(self, chunk_size):
        return _ChunkStream(self, chunk_size, allow_resplit=True)


class ItemStream(Stream):
    """
        Represents a stream of blobs, where each blob must NOT be split in the middle.
    """

    def into_chunks(self, chunk_size):
        return _ChunkStream(self, chunk_size, allow_resplit=False)


class _ChunkStream(Stream):
    def __init__(self, input, chunk_size, allow_resplit=False, merge_items_into_chunks=_merge_items_into_chunks):
        isatty = _stream_isatty(input)

        # NB: if stream is empty, we still want to make an empty write, e.g. for `write_table(name, [])`.
        # So, if stream is empty, we explicitly make it b"".
        input = _stream_or_empty_bytes(input)
        if allow_resplit:
            input = _resplit_chunks(input, chunk_size)
        else:
            input = merge_items_into_chunks(input, chunk_size)
        self.chunk_size = chunk_size

        super(_ChunkStream, self).__init__(input, isatty)

    @classmethod
    def _make_from_raw_stream(cls, input, chunk_size):
        stream = cls([], chunk_size)
        stream._iter = iter(input)
        return stream

    def split_chunks(self, piece_max_size):
        return ChunkGroupStream(self, self.chunk_size, piece_max_size)


class ChunkGroupStream(Stream):
    def __init__(self, chunks, chunk_size, piece_max_size):
        isatty = _stream_isatty(chunks)
        stream = _split_chunks_by_max_size(chunks, piece_max_size)
        self.chunk_size = chunk_size
        self.piece_max_size = piece_max_size
        super(ChunkGroupStream, self).__init__(stream, isatty)

    def _check_item_type(self, item):
        if not isinstance(item, list):
            raise TypeError("Stream is expected to contain list, but contains: '{}'".format(
                type(item)
            ))

    def flatten(self):
        chunks = _flatten_stream(self)
        return _ChunkStream._make_from_raw_stream(chunks, self.piece_max_size)
