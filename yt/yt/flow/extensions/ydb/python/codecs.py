import ydb
import zstandard

MAX_DECOMPRESSED_MESSAGE_SIZE = 1024**3
UNKNOWN_SIZE_COUNTING_CHUNK_SIZE = 64 * 1024


def decode_zstd(compressed: bytes) -> bytes:
    content_size = zstandard.frame_content_size(compressed)
    if content_size > MAX_DECOMPRESSED_MESSAGE_SIZE:
        raise zstandard.ZstdError('Decompressed message exceeds the size limit')
    decompressor = zstandard.ZstdDecompressor()
    if content_size != -1:
        return decompressor.decompress(compressed, max_output_size=MAX_DECOMPRESSED_MESSAGE_SIZE)
    content_size = 0
    for chunk in decompressor.read_to_iter(compressed, write_size=UNKNOWN_SIZE_COUNTING_CHUNK_SIZE):
        content_size += len(chunk)
        if content_size > MAX_DECOMPRESSED_MESSAGE_SIZE:
            raise zstandard.ZstdError('Decompressed message exceeds the size limit')
    return decompressor.decompress(compressed, max_output_size=max(content_size, 1))


def encode_zstd(data: bytes) -> bytes:
    return zstandard.ZstdCompressor().compress(data)


DECODERS = {ydb.TopicCodec.ZSTD: decode_zstd}
ENCODERS = {ydb.TopicCodec.ZSTD: encode_zstd}
