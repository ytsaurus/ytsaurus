import os
import threading
import datetime as dt

from exts import fs
from exts import uniq_id
from exts import filelock


__all__ = ['Queue']


DATE_FMT = '%Y_%m_%d_%H_%M_%S'
UNIQ_ID = 0


def uniq_name():
    global UNIQ_ID
    UNIQ_ID += 1
    return "{}__{:08d}__{}".format(dt.datetime.utcnow().strftime(DATE_FMT), UNIQ_ID, uniq_id.gen8())


def unpack_date(uniq_id):
    date, _, _ = uniq_id.split('__')
    return dt.datetime.strptime(date, DATE_FMT)


class Chunk(object):
    def __init__(self, data_dir, tag):
        self._data_dir = data_dir
        self._tag = tag
        self._data_path = os.path.join(data_dir, tag)
        self._lock = threading.Lock()
        self._stream = None

    @property
    def tag(self):
        return self._tag

    @property
    def stamp(self):
        return unpack_date(self._tag)

    def open(self):
        with self._lock:
            if self._stream is not None:
                raise RuntimeError('Already opened')
            self._stream = open(self._data_path, 'w+')

    def close(self):
        with self._lock:
            if self._stream is None:
                raise RuntimeError('Stream is not opened')
            self._stream.close()

    def consume(self, action):
        with self._lock:
            if self._stream is None:
                with open(self._data_path, 'r') as f:
                    for line in f:
                        for x in action(line):
                            yield x
                fs.remove_file(self._data_path)
            else:
                self._stream.seek(0)
                for line in self._stream:
                    for x in action(line):
                        yield x
                self._stream.seek(0)
                self._stream.truncate()
                self._stream.flush()

    def analyze(self, analyzer):
        self.flush()
        with self._lock:
            with open(self._data_path, 'r') as f:
                for line in f:
                    for x in analyzer(line):
                        yield x

    def consume_lines(self, lines_filter):
        with self._lock:
            class reopener:
                def __enter__(rself):
                    rself._opened = self._stream is not None
                    if rself._opened:
                        self._stream.close()
                        self._stream = None

                def __exit__(rself, type, value, traceback):
                    if rself._opened:
                        self._stream = open(self._data_path, 'a')
                    return isinstance(value, OSError)

            with reopener():
                with open(self._data_path, 'r+') as f:
                    left_over = [x for line in f for x in lines_filter(line)]
                    if left_over:
                        f.seek(0)
                        data = ''.join(left_over)
                        f.write(data)
                        f.truncate()

                if not left_over:
                    fs.remove_file(self._data_path)

    def add(self, value):
        with self._lock:
            if self._stream is None:
                raise RuntimeError('Chunk is not opened')
            self._stream.write(value + '\n')

    def flush(self):
        with self._lock:
            if self._stream is not None:
                self._stream.flush()

    @staticmethod
    def create_new(data_dir):
        return Chunk(data_dir, uniq_name())


class NoChunkError(Exception):
    pass


class Queue(object):
    def __init__(self, store_dir):
        fs.create_dirs(store_dir)

        self._data_dir = os.path.join(store_dir, 'data')
        self._consume_lock = filelock.FileLock(os.path.join(store_dir, 'consume.lock'))
        fs.create_dirs(self._data_dir)
        self._active_chunk = Chunk.create_new(self._data_dir)
        self._active_chunk.open()

    def close(self):
        self._active_chunk.close()

    def sieve(self, consumer, max_chunks=None):
        with self._consume_lock:
            for chunk_name in sorted(os.listdir(self._data_dir))[:max_chunks]:
                if os.path.basename(chunk_name) != self._active_chunk.tag:
                    chunk = Chunk(self._data_dir, chunk_name)
                    for x in chunk.consume(consumer):
                        yield x

    def sieve_current_chunk(self, consumer):
        with self._consume_lock:
            for x in self._active_chunk.consume(consumer):
                yield x

    def analyze(self, analyzer):
        with self._consume_lock:
            for chunk_name in sorted(os.listdir(self._data_dir)):
                chunk = Chunk(self._data_dir, chunk_name) \
                    if os.path.basename(chunk_name) != self._active_chunk.tag else self._active_chunk
                for x in chunk.analyze(analyzer):
                    yield x

    def push(self, value):
        self._active_chunk.add(value)

    def flush(self):
        self._active_chunk.flush()

    def strip(self, lines_filter):
        with self._consume_lock:
            for chunk_name in sorted(os.listdir(self._data_dir)):
                chunk = Chunk(self._data_dir, chunk_name) \
                    if os.path.basename(chunk_name) != self._active_chunk.tag else self._active_chunk
                chunk.consume_lines(lines_filter)
