import yt.wrapper as yt

def convert_to_erasure(src, dst=None, erasure_codec=None, compression_codec=None, desired_chunk_size=None, yt_client=None, spec=None):
    if erasure_codec is None or erasure_codec == "none":
        return False
    return transform(src, dst, erasure_codec, compression_codec, desired_chunk_size, yt_client, spec)

def transform(src, dst=None, erasure_codec=None, compression_codec=None, desired_chunk_size=None, yt_client=None, spec=None, check_codecs=False):
    return yt.transform(
        src,
        dst,
        erasure_codec=erasure_codec,
        compression_codec=compression_codec,
        desired_chunk_size=desired_chunk_size,
        spec=spec,
        check_codecs=check_codecs,
        client=yt_client)
