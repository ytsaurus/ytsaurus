from six.moves import xrange

import threading
import logging
import queue

logger = logging.getLogger("Odin")


class AsyncStorageWriter(object):
    def __init__(self, storage, max_batch_size):
        assert max_batch_size > 0
        self._max_batch_size = max_batch_size
        self._storage = storage
        self._queue = queue.Queue(maxsize=2**10)
        self._failed = False
        self._worker = threading.Thread(target=self._worker_main)
        self._worker.daemon = True
        self._worker.start()

    def _worker_main(self):
        try:
            self._worker_main_impl()
        except:  # noqa
            self._failed = True
            logger.exception("Error in AsyncStorageWriter")
            raise

    def _worker_main_impl(self):
        try:
            import prctl
            prctl.set_name("StorageWriter")
        except ImportError:
            pass

        while True:
            records = []
            records.append(self._queue.get())  # This blocks.
            for _ in xrange(self._max_batch_size - 1):
                try:
                    records.append(self._queue.get_nowait())
                except queue.Empty:
                    break
            self._storage.add_records_bulk(records)

    def add_record(self, **record):
        if self._failed:
            raise RuntimeError("AsyncStorageWriter worker thread failed")
        # NB: this may raise queue.Full exception.
        self._queue.put_nowait(record)
