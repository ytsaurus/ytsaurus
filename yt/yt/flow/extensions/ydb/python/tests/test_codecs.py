import os

import pytest
import ydb
import zstandard

from yt.yt.flow.extensions.ydb.python import codecs

SMALL_BOUND = 64


class RecordingDecompressor:
    def __init__(self):
        self.max_output_sizes = []
        self.streaming_write_sizes = []
        self.constructions = 0

    def __call__(self, *args, **kwargs):
        self.constructions += 1
        decompressor = REAL_DECOMPRESSOR(*args, **kwargs)
        recorder = self

        class Recorded:
            def decompress(self, data, max_output_size=0):
                recorder.max_output_sizes.append(max_output_size)
                return decompressor.decompress(data, max_output_size=max_output_size)

            def read_to_iter(self, data, **kwargs):
                recorder.streaming_write_sizes.append(kwargs["write_size"])
                return decompressor.read_to_iter(data, **kwargs)

        return Recorded()


REAL_DECOMPRESSOR = zstandard.ZstdDecompressor


@pytest.fixture
def recording_decompressor(monkeypatch):
    recorder = RecordingDecompressor()
    monkeypatch.setattr(zstandard, "ZstdDecompressor", recorder)
    return recorder


@pytest.fixture
def small_bound(monkeypatch):
    monkeypatch.setattr(codecs, "MAX_DECOMPRESSED_MESSAGE_SIZE", SMALL_BOUND)


def compress(size, write_content_size):
    data = os.urandom(size)
    frame = zstandard.ZstdCompressor(write_content_size=write_content_size).compress(data)
    expected_content_size = size if write_content_size else -1
    assert zstandard.frame_content_size(frame) == expected_content_size
    return data, frame


def test_codec_tables_cover_only_zstd():
    assert set(codecs.DECODERS) == {ydb.TopicCodec.ZSTD}
    assert set(codecs.ENCODERS) == {ydb.TopicCodec.ZSTD}


def test_zstd_round_trip_uses_one_gibibyte_bound(recording_decompressor):
    payload = b"flow" * 1000

    assert codecs.DECODERS[ydb.TopicCodec.ZSTD](codecs.ENCODERS[ydb.TopicCodec.ZSTD](payload)) == payload
    assert codecs.MAX_DECOMPRESSED_MESSAGE_SIZE == 1024**3
    assert recording_decompressor.max_output_sizes == [1024**3]


@pytest.mark.parametrize("write_content_size", [True, False])
def test_zstd_frame_at_bound_decodes(small_bound, recording_decompressor, write_content_size):
    data, frame = compress(SMALL_BOUND, write_content_size)

    assert codecs.decode_zstd(frame) == data
    assert recording_decompressor.max_output_sizes == [SMALL_BOUND]


def test_zstd_unknown_size_frame_decompresses_into_counted_size(recording_decompressor):
    data, frame = compress(1000, write_content_size=False)

    assert codecs.decode_zstd(frame) == data
    assert recording_decompressor.max_output_sizes == [len(data)]
    assert recording_decompressor.streaming_write_sizes == [codecs.UNKNOWN_SIZE_COUNTING_CHUNK_SIZE]


def test_zstd_unknown_size_frame_larger_than_counting_chunk_decodes(recording_decompressor):
    data, frame = compress(3 * codecs.UNKNOWN_SIZE_COUNTING_CHUNK_SIZE + 1, write_content_size=False)

    assert codecs.decode_zstd(frame) == data
    assert recording_decompressor.max_output_sizes == [len(data)]


def test_zstd_known_oversize_frame_rejected_before_decompressor(small_bound, recording_decompressor):
    _, frame = compress(SMALL_BOUND + 1, write_content_size=True)

    with pytest.raises(zstandard.ZstdError):
        codecs.decode_zstd(frame)
    assert recording_decompressor.constructions == 0


def test_zstd_unknown_oversize_frame_rejected_by_bounded_streaming(small_bound, recording_decompressor):
    _, frame = compress(SMALL_BOUND + 1, write_content_size=False)

    with pytest.raises(zstandard.ZstdError):
        codecs.decode_zstd(frame)
    assert recording_decompressor.streaming_write_sizes == [codecs.UNKNOWN_SIZE_COUNTING_CHUNK_SIZE]
    assert recording_decompressor.max_output_sizes == []


def test_zstd_unknown_size_truncated_frame_raises_zstd_error():
    _, frame = compress(1000, write_content_size=False)

    with pytest.raises(zstandard.ZstdError):
        codecs.decode_zstd(frame[:-1])


def test_zstd_invalid_header_raises_zstd_error():
    with pytest.raises(zstandard.ZstdError):
        codecs.decode_zstd(b"not a zstd frame")
