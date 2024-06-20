from yt.wrapper.common import YtError, GB, chunk_iter_string
import yt.logger as logger

import struct

try:
    import zlib_fork_safe as zlib
    _ZLIB_FORK_SAFE = True
except ImportError:
    import zlib
    _ZLIB_FORK_SAFE = False


def is_zlib_parallel():
    # NB: zlib in non-Arcadia Python 2.7 is single-threaded.
    # return PY3 or is_arcadia_python() or _ZLIB_FORK_SAFE
    return True


def try_enable_parallel_write_gzip(config_enable):
    enable = config_enable
    if enable is None:
        enable = is_zlib_parallel()
        if not enable:
            logger.debug("Parallel write is disabled because zlib is not parallel")
    elif enable and not is_zlib_parallel():
        logger.warning("Parallel write may be ineffective because zlib is not parallel")
    return enable


_CODECS = {}


class _Compressor(object):
    def __init__(self, compress_func, finish_func):
        self.compress_func = compress_func
        self.finish_func = finish_func

    def __call__(self, obj):
        if isinstance(obj, bytes):
            obj_iterator = chunk_iter_string(obj, 2 * GB)
        else:
            obj_iterator = obj

        for chunk in obj_iterator:
            compressed = self.compress_func(chunk)
            if compressed:
                yield compressed

        tail = self.finish_func()
        if tail:
            yield tail


def get_compressor(codec_name):
    if codec_name not in _CODECS:
        raise YtError('Compression module for codec "{0}" not found. Make sure you have '
                      'installed all necessary packages'.format(codec_name))
    return _CODECS[codec_name]()


def has_compressor(codec_name):
    if codec_name == "br" and codec_name in _CODECS:
        import brotli
        return hasattr(brotli.Compressor, "process")
    return codec_name in _CODECS


def _create_zlib_compressor():
    zlib_obj = zlib.compressobj()
    return _Compressor(zlib_obj.compress, zlib_obj.flush)


_CODECS["gzip"] = _create_zlib_compressor


try:
    import brotli

    def _create_brotli_compressor(quality=3):
        inner_compressor = brotli.Compressor(quality=quality)
        if not hasattr(inner_compressor, "process"):
            raise YtError("You use outdated version of brotli (probable deprecated package with named 'brotlipy'), "
                          "please update it (or completely remove)")
        return _Compressor(inner_compressor.process, inner_compressor.finish)

    _CODECS["br"] = _create_brotli_compressor
except ImportError:
    pass

try:
    import library.python.codecs

    class _BlockCompressor(object):
        def __init__(self, name):
            self._name = name
            self._codec_id = library.python.codecs.get_codec_id(self._name)

        def __call__(self, obj):
            if isinstance(obj, bytes):
                obj_iterator = chunk_iter_string(obj, 2 * GB)
            else:
                obj_iterator = obj

            for chunk in obj_iterator:
                compressed = library.python.codecs.dumps(self._name, chunk)
                if compressed:
                    yield struct.pack("<H", self._codec_id)
                    yield struct.pack("<Q", len(compressed))
                    yield compressed
            yield struct.pack("<H", self._codec_id)
            yield struct.pack("<Q", 0)

    _CODECS["z-lz4"] = lambda: _BlockCompressor("lz4")
except ImportError:
    pass
