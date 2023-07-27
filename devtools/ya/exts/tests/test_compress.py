import os
import pytest

from exts.compress import UCompressor
import library.python.compress as lpc
import yatest.common as yac


def test_ucompressor_with():
    orig_contents = b'a' * (1 << 10) + b'z' * (1 << 10)
    file_compressed = yac.test_output_path('file.uc')
    with UCompressor(file_compressed, 'zstd_1') as fp:
        fp.write(orig_contents)

    file_uncompressed = yac.test_output_path('file')
    lpc.decompress(file_compressed, file_uncompressed)
    assert len(orig_contents) == os.path.getsize(file_uncompressed)
    assert orig_contents == open(file_uncompressed, 'rb').read()
    assert len(orig_contents) > os.path.getsize(file_compressed)


def test_ucompressor_manual():
    orig_contents = b'a' * (1 << 10) + b'z' * (1 << 10)
    file_compressed = yac.test_output_path('file.uc')
    uc = UCompressor(file_compressed, 'zstd_1')
    assert not uc.isStarted()
    uc.start()
    assert uc.isStarted()
    uc.getInputStream().write(orig_contents)
    assert uc.isStarted()
    uc.stop()
    assert uc.isStarted()
    assert uc.getInputStream() is None

    file_uncompressed = yac.test_output_path('file')
    lpc.decompress(file_compressed, file_uncompressed)
    assert len(orig_contents) == os.path.getsize(file_uncompressed)
    assert orig_contents == open(file_uncompressed, 'rb').read()
    assert len(orig_contents) > os.path.getsize(file_compressed)


def test_ucompressor_asserts():
    file_compressed = yac.test_output_path('file.uc')
    uc = UCompressor(file_compressed, 'zstd_1')
    with pytest.raises(AssertionError):
        uc.getInputStream()
    assert not uc.isStarted()

    with pytest.raises(AssertionError):
        uc.stop()
    uc.start()
    assert uc.isStarted()
    assert uc.getInputStream() is not None

    with pytest.raises(AssertionError):
        uc.start()
    assert uc.isStarted()

    uc.stop()
    assert uc.isStarted()
    assert uc.getInputStream() is None
    with pytest.raises(AssertionError):
        uc.stop()
