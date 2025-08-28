import io  # noqa
import logging
import os
import threading
import typing  # noqa
import zlib

import library.python.compress as lpc


class UCompressor(object):
    def __init__(self, filename, codec, mode=None):
        rd, wd = os.pipe()
        self.uc_stream = os.fdopen(rd, 'rb')

        def _open(path, mode_):
            if 'r' in mode_:
                return self.uc_stream
            return open(path, mode_)

        logging.getLogger('compress').setLevel(logging.ERROR)
        self.uc_thread = threading.Thread(
            target=lpc.compress, args=(None, filename), kwargs={'codec': codec, 'fopen': _open}
        )
        self.uc_thread.daemon = True
        self.uc_thread_started = False
        self.in_stream = os.fdopen(wd, mode or 'wb')

    def getInputStream(self):
        assert self.uc_thread_started, 'not running'
        return self.in_stream

    def isStarted(self):
        return self.uc_thread_started

    def start(self):
        assert not self.uc_thread_started, 'can only be started once'
        assert not self.uc_thread.is_alive()
        self.uc_thread_started = True
        self.uc_thread.start()

    def stop(self):
        assert self.uc_thread_started, 'not running'
        assert self.uc_thread.is_alive()
        try:
            try:
                self.in_stream.close()
            finally:
                self.in_stream = None
                self.uc_thread.join()
        finally:
            self.uc_stream.close()

    def __enter__(self):
        self.start()
        return self.getInputStream()

    def __exit__(self, *exc_details):
        self.stop()


def ucopen(filename, mode=None):
    # Default result file-object mode is 'wb' for compressed and 'w' for raw file.
    # Though it makes Python 3 usage inconsistent, it is made to preserve compatibility.
    return UCompressor(filename, 'zstd_1', mode or 'wb') if filename.endswith('.uc') else open(filename, mode or 'w')


class ZLibCompressor(object):
    def __init__(self, fout, close_stream=True):
        # type: (io.IOBase, bool|None) -> None
        self._fout = fout
        self._closed = False
        self._close_stream = close_stream
        self._compressor = zlib.compressobj()

    def write(self, b):
        # type: (bytes) -> int
        assert not self._closed, "closed"
        self._fout.write(self._compressor.compress(b))
        return len(b)

    @property
    def closed(self):
        # type: () -> bool
        return self._closed

    def close(self):
        # type: () -> None
        if self._closed:
            return
        self._closed = True
        self._fout.write(self._compressor.flush())
        self._fout.flush()
        if self._close_stream:
            self._fout.close()

    def __enter__(self):
        # type: () -> typing.Self
        return self

    def __exit__(self, *exc_details):
        self.close()


def zcopen(filename):
    # type: (str) -> ZLibCompressor
    return ZLibCompressor(open(filename, "wb"))
