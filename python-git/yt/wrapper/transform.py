from .common import get_value, update
from .errors import YtResponseError
from .config import get_config
from .cypress_commands import get, set, list, exists, create
from .table_commands import run_merge
from .table import TablePath, TempTable, to_name

import yt.logger as logger

from copy import deepcopy

def _get_compression_ratio(table, codec, client, spec):
    logger.debug("Compress sample of '%s' to calculate compression ratio", table)
    with TempTable(prefix="compute_compression_ratio", client=client) as tmp:
        spec = update(deepcopy(spec), {
            "title": "Merge to calculate compression ratio",
            "force_transform": True,
        })
        set(tmp + "/@compression_codec", codec, client=client)
        chunk_index = get_config(client)["transform_options"]["chunk_count_to_compute_compression_ratio"]
        run_merge(TablePath(table, ranges=[{"upper_limit": {"chunk_index": chunk_index}}], client=client), tmp, mode="unordered", spec=spec, client=client)
        return get(table + "/@compression_ratio", client=client)

def _check_codec(table, codec_name, codec_value, client):
    if codec_value is None:
        return True
    else:
        try:
            codecs = list("{0}/@{1}_statistics".format(table, codec_name), client=client)
            return codecs == [codec_value]
        except YtResponseError as error:
            if error.is_resolve_error():
                return False
            else:
                raise

def transform(source_table, destination_table=None, erasure_codec=None, compression_codec=None, desired_chunk_size=None, spec=None, check_codecs=False, client=None):
    """ Transforms source_table table to destination_table writing data with given compression and erasure codecs.
    Automatically calculates desired chunk size and data size per job.
    """

    spec = get_value(spec, {})

    src = to_name(source_table, client=client)
    dst = None
    if destination_table is not None:
        dst = to_name(destination_table, client=client)

    if desired_chunk_size is None:
        desired_chunk_size = get_config(client)["transform_options"]["desired_chunk_size"]

    if not exists(src, client=client) or get(src + "/@row_count", client=client) == 0:
        logger.debug("Table %s is empty", src)
        return False

    if dst is None:
        dst = src
    else:
        create("table", dst, ignore_existing=True, client=client)

    if compression_codec is not None:
        ratio = _get_compression_ratio(src, compression_codec, spec=spec, client=client)
        set(dst + "/@compression_codec", compression_codec, client=client)
    else:
        ratio = get(src + "/@compression_ratio", client=client)

    if erasure_codec is not None:
        set(dst + "/@erasure_codec", erasure_codec, client=client)
    else:
        erasure_codec = get(dst + "/@erasure_codec", client=client)

    if check_codecs and _check_codec(dst, "compression", compression_codec, client=client) and _check_codec(dst, "erasure", erasure_codec, client=client):
        logger.debug("Table %s already has proper codecs", dst)
        return False

    data_size_per_job = max(
        1,
        min(
            get_config(client)["transform_options"]["max_data_size_per_job"],
            int(desired_chunk_size / ratio)
        )
    )

    spec = update(
        {
            "title": "Transform table",
            "combine_chunks": True,
            "force_transform": True,
            "data_size_per_job": data_size_per_job,
            "job_io": {
                "table_writer": {
                    "desired_chunk_size": desired_chunk_size
                }
            }
        },
        spec)

    logger.debug("Transform from '%s' to '%s' (spec: '%s')", src, dst, spec)
    if client is not None:
        # NB: Not passing client with kwarg here because Transfer Manager
        # patches run_* methods in YT client to track task progress.
        client.run_merge(src, dst, spec=spec)
    else:
        run_merge(src, dst, spec=spec)
