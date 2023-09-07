__all__ = ['ChunkedQueue']

import os
import threading
import exts.yjson as json
import random
import logging
import datetime as dt

from exts import fs
from exts import filelock
from exts import uniq_id

logger = logging.getLogger(__name__)


def uniq_name():
    return "{}_{}".format(dt.datetime.utcnow().strftime('%Y_%m_%d_%H_%M_%S'), uniq_id.gen8())


class ActiveChunk(object):
    def __init__(self, data_path, lock_path):
        self._stream = self.__open_stream(data_path)
        self._flock = filelock.FileLock(lock_path)
        self._flock.acquire()
        self._lock = threading.Lock()

    def __del__(self):
        with self._lock:
            self._stream.close()

    def add(self, dct):
        with self._lock:
            self._stream.write(json.dumps(dct) + '\n')
            self._stream.flush()

    def __open_stream(self, data_path):
        if os.path.exists(data_path):
            os.remove(data_path)

        try:
            old_mask = os.umask(0o177)
            fd = os.open(data_path, os.O_CREAT | os.O_WRONLY, 0o600)
            return os.fdopen(fd, 'w')
        finally:
            os.umask(old_mask)


class ChunkedQueue(object):
    def __init__(self, store_dir):
        self._data_dir = os.path.join(store_dir, 'data')
        self._locks_dir = os.path.join(store_dir, 'locks')
        fs.create_dirs(self._data_dir)
        fs.create_dirs(self._locks_dir)
        self._active_tag = uniq_name()
        self._active_chunk = ActiveChunk(
            os.path.join(self._data_dir, self._active_tag),
            os.path.join(self._locks_dir, self._active_tag)
        )

    def cleanup(self, max_items=None):
        data_lst = os.listdir(self._data_dir)
        lock_lst = os.listdir(self._locks_dir)
        dangling_locks = set(lock_lst) - set(data_lst)
        for x in dangling_locks:
            self._try_remove(os.path.join(self._locks_dir, x))
        if max_items is not None:
            for x in sorted(data_lst)[:-max_items]:
                self._try_remove(os.path.join(self._data_dir, x))
                self._try_remove(os.path.join(self._locks_dir, x))

    def add(self, dct):
        self._active_chunk.add(dct)

    def consume(self, consumer):
        to_sync = [x for x in os.listdir(self._data_dir) if x != self._active_tag]
        random.shuffle(to_sync)
        for name in to_sync:
            path = os.path.join(self._data_dir, name)
            try:
                consumer([json.loads(x) for x in fs.read_file(path).splitlines()])
            except Exception:
                import traceback
                logger.debug(traceback.format_exc())
            else:
                with filelock.FileLock(os.path.join(self._locks_dir, name)):  # XXX: use non blocking interface
                    self._try_remove(path)

    @staticmethod
    def _try_remove(path):
        try:
            fs.remove_file(path)
        except OSError:
            pass
