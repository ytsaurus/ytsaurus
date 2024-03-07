#!/usr/bin/python

from .logger import logger
from .test_ordered import test_ordered_tables
from .test_sorted import test_sorted_tables
from .stateless_write import run_stateless_writer
from .test_compare_replicas import run_compare_replicas
from .parser import parse_args
from .spec import variate_modes, Spec

import yt.wrapper as yt
from yt.wrapper.http_helpers import get_token
import random
import sys
import traceback
import pprint
import warnings
import string


yt.config["driver_logging_config"] = {
    "rules": [
        {
            "min_level": "warning",
            "writers": [
                "stderr",
            ],
        },
    ],
    "writers": {
        "stderr": {
            "type": "stderr",
        },
    },
}

yt.config["pickling"]["module_filter"] = (
    lambda module: "hashlib" not in getattr(module, "__name__", "") and
    getattr(module, "__name__", "") != "hmac" and
    "numpy" not in getattr(module, "__name__", "")
)

yt.config["dynamic_table_retries"] = {
    "enable": True, "count": 10,
    "backoff": {"policy": "constant_time", "constant_time": 200},
}

yt.config["tablets_ready_timeout"] = 300 * 1000

yt.config["read_progress_bar"] = {"enable": False}
yt.config["write_progress_bar"] = {"enable": False}

################################################################################

def configure_client(client):
    client.config["spec_defaults"] = {
        "secure_vault": {"YT_TOKEN": get_token()},
        "enable_partitioned_data_balancing": False,
        "mapper": {
            "memory_limit": 1024 * 1024 * 1024 * 5,
        },
        "reducer": {
            "memory_limit": 1024 * 1024 * 1024 * 5,
        },
        "max_speculative_job_count_per_task": 0,
        "auto_merge": {"mode": "disabled"},
    }
    client.config["operation_tracker"]["progress_logging_level"] = "DEBUG"
    client.config["operation_tracker"]["stderr_logging_level"] = "DEBUG"

def prepare_attributes(spec):
    attributes = {
        "chunk_format": spec.chunk_format,
        "erasure_codec": spec.erasure_codec,
        "in_memory_mode": spec.in_memory_mode,
        "enable_lsm_verbose_logging": True,
        "chunk_writer": {"erasure_store_original_block_checksums": True},
        "mount_config": {},
    }

    if spec.compression_codec is not None:
        attributes["compression_codec"] = spec.compression_codec

    if spec.extra_attributes:
        attributes.update(spec.extra_attributes.to_dict() or {})

    if spec.table_type == "sorted":
        if spec.sorted.enable_lookup_hash_table and spec.in_memory_mode == "uncompressed":
            attributes["enable_lookup_hash_table"] = True
        if spec.sorted.enable_data_node_lookup:
            attributes["enable_data_node_lookup"] = True
        if spec.sorted.lookup_cache_rows_per_tablet is not None:
            attributes["lookup_cache_rows_per_tablet"] = spec.sorted.lookup_cache_rows_per_tablet
        if spec.sorted.enable_hash_chunk_index_for_lookup:
            attributes["mount_config"]["enable_hash_chunk_index_for_lookup"] = True
        if spec.sorted.enable_value_dictionary_compression:
            attributes["mount_config"]["value_dictionary_compression"] = {"enable": True}

    return attributes

def run_with_spec(base_path, spec, args):
    yt.write_file(base_path + "/spec", pprint.pformat(spec.to_dict()).encode())
    attributes = prepare_attributes(spec)

    if spec.mode == "iterative":
        logger.iteration = None
        if spec.table_type == "sorted":
            test_sorted_tables(base_path, spec, attributes, args.force)
        else:
            test_ordered_tables(base_path, spec, attributes, args)
    elif spec.mode == "stateless_write":
        assert False
        run_stateless_writer(base_path, spec, attributes, args)
    elif spec.mode == "compare_replicas":
        run_compare_replicas(base_path, spec, attributes, args)

def set_seed(seed):
    random.seed(seed)

    try:
        import numpy.random as np_random
        np_random.seed(seed)
    except ModuleNotFoundError:
        pass

def run_test(spec, args):
    if args.random_name:
        r = random.Random()
        base_path = args.path + "/" + "".join(r.choice(string.ascii_letters) for i in range(6))
        logger.info("Creating base directory {}".format(base_path))
        yt.create("map_node", base_path, force=args.force)
    else:
        base_path = args.path

    for repeat in range(args.repeats):
        logger.generation = repeat

        seed = Spec(spec).seed
        if seed is None:
            seed = random.randint(1, 10**9)
            logger.info("Your seed is %s", seed)
        seed += repeat

        modes = variate_modes(spec)

        for mode_index, (resulting_spec, description) in enumerate(modes):
            print("Variated options:")
            for k, v in description:
                print("   ", k, v)
            print()
            print("Resulting spec:")
            pprint.pprint(resulting_spec)
            print()
            try:
                path = base_path
                suffixes = []
                if args.repeats > 1:
                    suffixes.append("repeat" + str(repeat))
                if len(modes) > 1:
                    suffixes.append("mode" + str(mode_index))
                if suffixes:
                    path = path + "/" + ".".join(suffixes)
                    logger.info("Creating subdirectory {}".format(path))
                    yt.create("map_node", path, force=args.force)
                run_with_spec(path, Spec(resulting_spec), args)
            except Exception as ex:
                sys.stdout.write(traceback.format_exc())
                logger.error("Test %s failed\n" % (base_path))

def main():
    warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)

    configure_client(yt)

    spec, args = parse_args()

    if args.show_spec:
        pprint.pprint(spec)
        return

    if args.pool is not None:
        yt.config["pool"] = args.pool
    if args.max_failed_job_count is not None:
        yt.config["spec_defaults"]["max_failed_job_count"] = args.max_failed_job_count

    run_test(spec, args)
