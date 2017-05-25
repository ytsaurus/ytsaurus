from yt.common import YtError

from yt.packages.six import binary_type

try:
    import zlib_fork_safe as zlib
except ImportError:
    import zlib

_CODECS = {}

class _Compressor(object):
    def __init__(self, compress_func, finish_func):
        self.compress_func = compress_func
        self.finish_func = finish_func

    def __call__(self, obj):
        if isinstance(obj, binary_type):
            yield self.compress_func(obj)
        else:
            for chunk in obj:
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

def _create_zlib_compressor():
    zlib_obj = zlib.compressobj()
    return _Compressor(zlib_obj.compress, zlib_obj.flush)

_CODECS["gzip"] = _create_zlib_compressor

try:
    import brotli

    def _create_brotli_compressor(quality=3):
        inner_compressor = brotli.Compressor(quality=quality)
        return _Compressor(inner_compressor.process, inner_compressor.finish)

    _CODECS["br"] = _create_brotli_compressor
except ImportError:
    pass
