from yt.packages.six import text_type, binary_type, next
from yt.packages.six.moves import xrange

import types


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
                      for start_index in xrange(0, len(chunk), max_size)]
            yield pieces

def _resplit_chunks(chunks, chunk_size):
    group = []
    length = 0
    for chunk in chunks:
        assert isinstance(chunk, (text_type, binary_type))
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
        assert isinstance(item, (text_type, binary_type))
        chunk_items.append(item)
        length += len(chunk_items[-1])
        if length >= chunk_size:
            yield b"".join(chunk_items)
            length = 0
            chunk_items = []
    if chunk_items:
        yield b"".join(chunk_items)

class Stream(object):
    item_types = ()

    def __iter__(self):
        return self

    def next(self):
        # Python 2 compatibility method.
        return self.__next__()

    def __next__(self):
        item = next(self._data)
        assert isinstance(item, self.item_types)
        return item

class ChunkStream(Stream):
    item_types = (text_type, binary_type)

    def __init__(self, data, chunk_size, allow_resplit=False):
        # NB: if stream is empty, we still want to make an empty write, e.g. for `write_table(name, [])`.
        # So, if stream is empty, we explicitly make it b"".
        data = _stream_or_empty_bytes(data)
        if allow_resplit:
            self._data = _resplit_chunks(data, chunk_size)
        else:
            self._data = _merge_items_into_chunks(data, chunk_size)
        self.chunk_size = chunk_size

    @classmethod
    def _make_from_raw_stream(self, data, chunk_size):
        stream = ChunkStream([], chunk_size)
        stream._data = data
        return stream

    def split_chunks(self, piece_max_size):
        return ChunkGroupStream(self, self.chunk_size, piece_max_size)

class ChunkGroupStream(Stream):
    item_types = (list,)

    def __init__(self, chunks, chunk_size, piece_max_size):
        self._data = _split_chunks_by_max_size(chunks, piece_max_size)
        self.chunk_size = chunk_size
        self.piece_max_size = piece_max_size

    def flatten(self):
        chunks = _flatten_stream(self)
        return ChunkStream._make_from_raw_stream(chunks, self.piece_max_size)
