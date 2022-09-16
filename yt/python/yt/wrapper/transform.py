from .common import get_value, update
from .errors import YtResponseError
from .config import get_config
from .cypress_commands import get, set, list, exists, create
from .run_operation_commands import run_merge
from .table import TempTable
from .transaction import Transaction
from .ypath import TablePath

import yt.logger as logger

try:
    from yt.packages.six.moves import xrange, map as imap, builtins
except ImportError:
    from six.moves import xrange, map as imap, builtins

from copy import deepcopy
from random import Random


def _get_compression_ratio(table, codec, client, spec):
    def exact_chunk_index_limit(chunk_index):
        return {"lower_limit": {"chunk_index": chunk_index}, "upper_limit": {"chunk_index": chunk_index + 1}}

    logger.debug("Compress sample of '%s' to calculate compression ratio", table)
    with TempTable(prefix="compute_compression_ratio", client=client) as tmp:
        spec = update(deepcopy(spec), {
            "title": "Merge to calculate compression ratio",
            "force_transform": True,
        })
        set(tmp + "/@compression_codec", codec, client=client)

        probe_chunk_count = get_config(client)["transform_options"]["chunk_count_to_compute_compression_ratio"]
        chunk_count = get(table + "/@chunk_count", client=client)

        random_gen = Random()
        random_gen.seed(chunk_count)
        chunk_indices = random_gen.sample(xrange(chunk_count), min(chunk_count, probe_chunk_count))
        input = TablePath(table,
                          ranges=builtins.list(imap(exact_chunk_index_limit, chunk_indices)),
                          client=client)

        run_merge(input, tmp, mode="ordered", spec=spec, client=client)
        return get(tmp + "/@compression_ratio", client=client)


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


def transform(source_table, destination_table=None, erasure_codec=None, compression_codec=None,
              desired_chunk_size=None, spec=None, check_codecs=False, optimize_for=None, client=None):
    """Transforms source table to destination table writing data with given compression and erasure codecs.

    Automatically calculates desired chunk size and data size per job. Also can be used to convert chunks in
    table between old and new formats (optimize_for parameter).
    """

    spec = get_value(spec, {})

    src = TablePath(source_table, client=client).to_yson_type()
    dst = None
    if destination_table is not None:
        dst = TablePath(destination_table, client=client).to_yson_type()

    if desired_chunk_size is None:
        desired_chunk_size = get_config(client)["transform_options"]["desired_chunk_size"]

    if not exists(src, client=client):
        logger.debug("Source table %s does not exist", src)
        return False

    with Transaction(client=client):
        if dst is None:
            dst = src
        else:
            create("table", dst, ignore_existing=True, client=client)

        if compression_codec is not None:
            set(dst + "/@compression_codec", compression_codec, client=client)

        if erasure_codec is not None:
            set(dst + "/@erasure_codec", erasure_codec, client=client)
        else:
            erasure_codec = get(dst + "/@erasure_codec", client=client)

        if optimize_for is not None:
            set(dst + "/@optimize_for", optimize_for, client=client)

        if get(src + "/@row_count", client=client) == 0:
            logger.debug("Table %s is empty", src)
            return False

        if check_codecs and \
                _check_codec(dst, "compression", compression_codec, client=client) and \
                _check_codec(dst, "erasure", erasure_codec, client=client) and \
                _check_codec(dst, "optimize_for", optimize_for, client=client):
            logger.info("Table %s already has proper codecs", dst)
            return False

        if compression_codec is not None:
            ratio = _get_compression_ratio(src, compression_codec, spec=spec, client=client)
        else:
            ratio = get(src + "/@compression_ratio", client=client)

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

    return True
