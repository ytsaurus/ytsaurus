from .common import get_value, update
from .errors import YtResponseError
from .config import get_config
from .cypress_commands import get, set, list, exists, create
from .default_config import ErasureCodecsType, CompressionCodecType
from .run_operation_commands import run_merge
from .spec_builders import SpecCommonType
from .table import TempTable
from .transaction import Transaction
from .ypath import TablePath, YPath

import yt.logger as logger

import builtins

from copy import deepcopy
from typing import Union, Optional, Dict, Any, Literal
from random import Random


def _get_compression_ratio(table, erasure_codec, compression_codec, optimize_for, spec, client):
    def exact_chunk_index_limit(chunk_index):
        return {"lower_limit": {"chunk_index": chunk_index}, "upper_limit": {"chunk_index": chunk_index + 1}}

    logger.debug("Compress sample of '%s' to calculate compression ratio", table)
    with TempTable(prefix="compute_compression_ratio", client=client) as tmp:
        spec = update(deepcopy(spec), {
            "title": "Merge to calculate compression ratio",
            "force_transform": True,
        })
        if compression_codec is not None:
            set(tmp + "/@compression_codec", compression_codec, client=client)

        if erasure_codec is not None:
            set(tmp + "/@erasure_codec", erasure_codec, client=client)

        if optimize_for is not None:
            set(tmp + "/@optimize_for", optimize_for, client=client)

        probe_chunk_count = get_config(client)["transform_options"]["chunk_count_to_compute_compression_ratio"]
        chunk_count = get(table + "/@chunk_count", client=client)

        random_gen = Random()
        random_gen.seed(chunk_count)
        chunk_indices = random_gen.sample(range(chunk_count), min(chunk_count, probe_chunk_count))
        input = TablePath(table,
                          ranges=builtins.list(map(exact_chunk_index_limit, chunk_indices)),
                          client=client)

        run_merge(input, tmp, mode="ordered", spec=spec, client=client)
        compressed_table = get(tmp, attributes=["data_weight", "compressed_data_size"], client=client)
        if compressed_table.attributes["compressed_data_size"] and compressed_table.attributes["data_weight"]:
            ratio = compressed_table.attributes["compressed_data_size"] / compressed_table.attributes["data_weight"]
        else:
            ratio = 0.0
        logger.debug("Estimated compression ratio of '%s' (codec: %s, erasure: %s, optimize: %s) is %s", table, compression_codec, erasure_codec, optimize_for, ratio)
        return None if ratio < 0.00001 else ratio


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


def transform(
    source_table: Union[str, YPath],
    destination_table: Optional[Union[str, YPath]] = None,
    erasure_codec: Optional[ErasureCodecsType] = None,
    compression_codec: Optional[CompressionCodecType] = None,
    desired_chunk_size: Optional[int] = None,
    spec: Optional[Union[SpecCommonType, Dict[str, Any]]] = None,
    check_codecs: bool = False,
    optimize_for: Optional[Literal["lookup", "scan"]] = None,
    force_empty: bool = False,
    client=None,
) -> bool:
    """Transforms source table to destination table writing data with given compression and erasure codecs.

    Automatically calculates desired chunk size and data size per job. Also can be used to convert chunks in
    table between old and new formats (optimize_for parameter).

    :param desired_chunk_size: Size of table chunks we are aiming for (size corresponds to `@compressed_data_size` attribute);
        this parameter implicitly disables job splitting (operation might run longer but we do not split output chunks into smaller parts)
    :param check_codecs: do not transform if "compression", "erasure" and "optimize_for" are relevant
    :param force_empty: tranform even empty table
    :param spec: extra operation parameters
    """  # noqa

    spec = get_value(spec, {})

    src = TablePath(source_table, client=client).to_yson_type()
    dst = None
    target_attributes = {}
    if destination_table is not None:
        dst = TablePath(destination_table, client=client).to_yson_type()
        if exists(dst, client=client):
            target_attributes = get(dst, attributes=["erasure_codec", "compression_codec", "optimize_for"], client=client).attributes
    else:
        if exists(src, client=client):
            target_attributes = get(src, attributes=["erasure_codec", "compression_codec", "optimize_for"], client=client).attributes

    if erasure_codec is None and target_attributes.get("erasure_codec"):
        erasure_codec = target_attributes["erasure_codec"]
        logger.debug("Using erasure codec from destination table: \"%s\"", erasure_codec)
    if compression_codec is None and target_attributes.get("compression_codec"):
        compression_codec = target_attributes["compression_codec"]
        logger.debug("Using compression codec from destination table: \"%s\"", compression_codec)
    if optimize_for is None and target_attributes.get("optimize_for"):
        optimize_for = target_attributes["optimize_for"]
        logger.debug("Using \"optimize for\" from destination table: \"%s\"", optimize_for)

    if desired_chunk_size is None:
        desired_chunk_size = get_config(client)["transform_options"]["desired_chunk_size"]
        job_splitting = None
    else:
        job_splitting = False

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

        src_attributes = get(src, attributes=["row_count", "dynamic", "chunk_row_count"], client=client).attributes
        if not force_empty and (
            src_attributes.get("row_count") == 0 or
            src_attributes.get("dynamic") and
            src_attributes.get("chunk_row_count") == 0
        ):
            logger.debug("Table %s is empty", src)
            return False

        if check_codecs and \
                _check_codec(dst, "compression", compression_codec, client=client) and \
                _check_codec(dst, "erasure", erasure_codec, client=client) and \
                _check_codec(dst, "optimize_for", optimize_for, client=client):
            logger.info("Table %s already has proper codecs", dst)
            return False

        ratio = _get_compression_ratio(src, erasure_codec, compression_codec, optimize_for, spec=spec, client=client) if compression_codec is not None else None

        if ratio is None:
            ratio = get(src + "/@compression_ratio", client=client)

        max_data_weight_per_job = get_config(client)["transform_options"]["max_data_size_per_job"]
        if ratio == 0.0:
            data_weight_per_job = min(1, max_data_weight_per_job)
        else:
            data_weight_per_job = max(1, min(max_data_weight_per_job, int(desired_chunk_size / ratio)))
            # force "one job - one chunk" mode (real chunk size based on data_size_per_job)
            desired_chunk_size = desired_chunk_size * 2 + 1024 ** 3

        base_spec = {
            "title": "Transform table",
            "combine_chunks": True,
            "force_transform": True,
            "job_io": {
                "table_writer": {
                    "desired_chunk_size": desired_chunk_size
                }
            },
        }
        if "data_size_per_job" not in spec and "data_weight_per_job" not in spec:
            base_spec["data_weight_per_job"] = data_weight_per_job

        spec = update(
            base_spec,
            spec,
        )

        # spread data_size/weight to override defaults from config:spec_defaults
        spec["data_size_per_job"] = spec["data_weight_per_job"] = spec.get("data_weight_per_job") or spec.get("data_size_per_job")

        if job_splitting is not None:
            spec.update({"enable_job_splitting": job_splitting})

        logger.debug("Transform from '%s' to '%s' (spec: '%s')", src, dst, spec)
        if client is not None:
            # NB: Not passing client with kwarg here because Transfer Manager
            # patches run_* methods in YT client to track task progress.
            client.run_merge(src, dst, spec=spec)
        else:
            run_merge(src, dst, spec=spec)

    return True
