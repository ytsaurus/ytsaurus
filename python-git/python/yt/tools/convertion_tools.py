import yt.logger as logger
import yt.wrapper as yt

def _get_compression_ratio(table, codec):
    logger.info("Compress sample of '%s' to calculate compression ratio", table) 
    tmp = yt.create_temp_table()
    yt.set(tmp + "/@compression_codec", codec)
    yt.run_merge(table + "[#1:#10000]", tmp, mode="unordered", spec={"force_transform": "true"})
    ratio = yt.get(table + "/@compression_ratio")
    yt.remove(tmp)
    return ratio

def _check_codec(table, codec_name, codec_value):
    if codec_value is None:
        return True
    else:
        try:
            codecs = yt.list("{0}/@{1}_statistics".format(table, codec_name))
            return codecs == [codec_value]
        except yt.YtResponseError as error:
            if error.is_resolve_error():
                return False
            else:
                raise

def convert_to_erasure(src, dst=None, erasure_codec=None, compression_codec=None, desired_chunk_size=None):
    if erasure_codec is None:
        pass

    if not yt.exists(src) or yt.get(src + "/@row_count") == 0:
        logger.info("Table is empty")
        return False

    if dst is None:
        dst = src
    else:
        yt.create("table", dst, ignore_existing=True)
    
    if compression_codec is not None:
        ratio = _get_compression_ratio(src, compression_codec)
        yt.set(dst + "/@compression_codec", compression_codec)
    else:
        ratio = yt.get(src + "/@compression_ratio")

    yt.set(dst + "/@erasure_codec", erasure_codec)
    if _check_codec(dst, "compression", compression_codec) and _check_codec(dst, "erasure", erasure_codec):
        logger.info("Table already has proper codecs")
        return False

    data_size_per_job = max(1, int(desired_chunk_size / ratio))
    mode = "sorted" if yt.is_sorted(src) else "unordered"
    
    spec = {"combine_chunks": "true",
            "force_transform": "true",
            "data_size_per_job": data_size_per_job,
            "job_io": {
                "table_writer": {
                    "desired_chunk_size": desired_chunk_size
                }
            }}
   
    logger.info("Merge from '%s' to '%s' (mode: '%s', spec: '%s'", src, dst, mode, spec) 
    yt.run_merge(src, dst, mode, spec=spec)

