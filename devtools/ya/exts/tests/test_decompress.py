import os
import pytest

from exts.decompress import UDecompressor
import library.python.compress as lpc
import yatest.common as yac


def test_udecompressor_with():
    orig_contents = b'a' * (1 << 10) + b'z' * (1 << 10)
    file_uncompressed = yac.test_output_path('file')
    with open(file_uncompressed, 'wb') as fp:
        fp.write(orig_contents)

    file_compressed = yac.test_output_path('file.uc')
    lpc.compress(file_uncompressed, file_compressed, codec='zstd_1')
    with open(file_compressed, 'rb') as fp:
        assert orig_contents != fp.read()

    with UDecompressor(file_compressed) as fp:
        assert orig_contents == fp.read()


def test_udecompressor_manual():
    orig_contents = b'a' * (1 << 10) + b'z' * (1 << 10)
    file_uncompressed = yac.test_output_path('file')
    with open(file_uncompressed, 'wb') as fp:
        fp.write(orig_contents)
    file_compressed = yac.test_output_path('file.uc')
    lpc.compress(file_uncompressed, file_compressed, codec='zstd_1')

    uc = UDecompressor(file_compressed)
    assert not uc.isStarted()
    uc.start()
    assert uc.isStarted()
    assert orig_contents == uc.getOutputStream().read()
    assert uc.isStarted()
    uc.stop()
    assert uc.isStarted()
    assert uc.getOutputStream() is None


def test_udecompressor_asserts():
    file_uncompressed = yac.test_output_path('file')
    with open(file_uncompressed, 'wb') as fp:
        fp.write(b'b' * (1 << 10))
    file_compressed = yac.test_output_path('file.uc')
    lpc.compress(file_uncompressed, file_compressed, codec='zstd_1')

    uc = UDecompressor(file_compressed)
    with pytest.raises(AssertionError):
        uc.getOutputStream()
    assert not uc.isStarted()

    with pytest.raises(AssertionError):
        uc.stop()
    uc.start()
    assert uc.isStarted()
    assert uc.getOutputStream() is not None

    with pytest.raises(AssertionError):
        uc.start()
    assert uc.isStarted()

    uc.stop()
    assert uc.isStarted()
    assert uc.getOutputStream() is None
    with pytest.raises(AssertionError):
        uc.stop()
