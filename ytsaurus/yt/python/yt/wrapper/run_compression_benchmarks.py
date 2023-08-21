import yt.wrapper as yt

import math
import json
import re
import sys
import time
import csv

DESCRIPTION = '''
Gets input table and recompresses it in all available codecs.

For each codec prints compression ratio, cpu_write and cpu_read.
'''

CODEC_WITH_LEVEL_PATTERN = re.compile(r"(.+)_(\d+)$")


def compute_row_count(attributes, sample_size):
    uncompressed_size = attributes["uncompressed_data_size"]
    row_count = attributes["row_count"]
    if row_count == 0:
        raise yt.YtError("Input table is empty.")

    average_row_size = uncompressed_size / row_count

    return min(row_count, int(math.ceil(sample_size / average_row_size)))


def get_compression_codec_list(client, return_all):
    features = client.get_supported_features()
    compression_codec_list = list(set(features["compression_codecs"]))
    if return_all:
        return compression_codec_list

    codecs = []
    codecs_with_levels = {}
    for codec in compression_codec_list:
        match = CODEC_WITH_LEVEL_PATTERN.match(codec)
        if not match:
            codecs.append(codec)
        else:
            name = match.group(1)
            level = int(match.group(2))
            codecs_with_levels.setdefault(name, []).append(level)

    for codec in codecs_with_levels:
        levels = sorted(codecs_with_levels[codec])
        min_ = levels[0]
        max_ = levels[-1]
        mid = levels[len(levels) // 2]
        codecs.append("{}_{}".format(codec, min_))
        codecs.append("{}_{}".format(codec, mid))
        codecs.append("{}_{}".format(codec, max_))

    return codecs


def get_value_by_path(node, path):
    tokens = path.split("/")
    for token in tokens:
        node = node[token]
    return node


def wait_available_operation(tracker, max_operations):
    while tracker.get_operation_count() >= max_operations:
        time.sleep(1)


def get_stats_list(stats):
    def sort_key(x):
        codec = x["codec"]
        match = CODEC_WITH_LEVEL_PATTERN.match(codec)
        if not match:
            return (codec, 0)
        else:
            name = match.group(1)
            level = int(match.group(2))
            return (name, level)

    stats_list = []
    for codec in stats:
        stats[codec]["codec"] = codec
        stats_list.append(stats[codec])

    stats_list.sort(key=sort_key)
    return stats_list


def check_timeout(operation_info):
    error_message = get_value_by_path(operation_info, "result/error/message")

    if "too long" not in error_message:
        raise yt.YtError("Operation finished unsuccessfully: {}".format(error_message))


def run(table, time_limit_sec, max_operations, sample_size, all_codecs, client=None):
    TIMED_OUT = "Timed out"
    NOT_LAUNCHED = "Not launched"

    client = yt.YtClient(config=yt.config.config)
    tmp_dir = client.config["remote_temp_tables_directory"] + "/compression-benchmarks"
    input_table_attributes = client.get(table + "/@")
    if input_table_attributes["dynamic"]:
        raise yt.YtError("Given table is dynamic. Benchmark can work only with static tables")

    sample_rows = compute_row_count(input_table_attributes, sample_size)
    optimize_for = input_table_attributes["optimize_for"]
    compression_codec_list = get_compression_codec_list(client, all_codecs)

    tx = client.start_transaction()
    with yt.Transaction(transaction_id=tx, client=client, ping=True):
        yt.mkdir(tmp_dir, recursive=True, client=client)
        with client.TempTable(tmp_dir) as sample_table:
            client.run_merge(
                client.TablePath(table, end_index=sample_rows),
                client.TablePath(sample_table, optimize_for=optimize_for),
                spec={"force_transform": True}
            )

            tracker = yt.OperationsTracker()
            temp_tables = []
            operation_id_list = []

            for codec in compression_codec_list:
                wait_available_operation(tracker, max_operations)
                dest_table = client.create_temp_table(path=tmp_dir)
                operation = client.run_merge(
                    sample_table,
                    client.TablePath(dest_table, compression_codec=codec, optimize_for=optimize_for),
                    mode="ordered",
                    spec={
                        "force_transform": True,
                        "job_count": 1,
                        "time_limit": 1000 * time_limit_sec,
                    },
                    sync=False,
                )
                tracker.add(operation)
                temp_tables.append(dest_table)
                operation_id_list.append(operation.id)

            tracker.wait_all(check_result=False)
            stats = {}

            for codec, table, operation_id in zip(compression_codec_list, temp_tables, operation_id_list):
                operation_info = client.get_operation_attributes(operation_id, fields=["progress", "state", "result"])
                operation_state = yt.operation_commands.OperationState(operation_info["state"])
                if operation_state.is_unsuccessfully_finished():
                    check_timeout(operation_info)
                    compression_ratio = TIMED_OUT
                    cpu_write = TIMED_OUT
                else:
                    attributes = client.get(table, attributes=["compression_ratio"]).attributes
                    compression_ratio = attributes["compression_ratio"]
                    cpu_write = get_value_by_path(
                        operation_info,
                        "progress/job_statistics/codec/cpu/encode/0/{}/$/completed/ordered_merge/sum".format(codec)
                    )
                stats[codec] = {}
                stats[codec]["compression_ratio"] = compression_ratio
                stats[codec]["codec/cpu/encode"] = cpu_write

            operation_id_list = []

            for (codec, table) in zip(compression_codec_list, temp_tables):
                if stats[codec]["compression_ratio"] == TIMED_OUT:
                    stats[codec]["codec/cpu/decode"] = NOT_LAUNCHED
                    operation_id_list.append(None)
                    continue

                wait_available_operation(tracker, max_operations)
                operation = client.run_map(
                    "cat > /dev/null",
                    source_table=table,
                    format=yt.YsonFormat(),
                    job_count=1,
                    spec={"time_limit": 1000 * time_limit_sec},
                    sync=False,
                )
                tracker.add(operation)
                operation_id_list.append(operation.id)

            tracker.wait_all(check_result=False)

            for codec, operation_id in zip(compression_codec_list, operation_id_list):
                if operation_id is None:
                    continue
                operation_info = client.get_operation_attributes(operation_id, fields=["progress", "state", "result"])
                operation_state = yt.operation_commands.OperationState(operation_info["state"])
                if operation_state.is_unsuccessfully_finished():
                    check_timeout(operation_info)
                    cpu_read = TIMED_OUT
                else:
                    cpu_read = get_value_by_path(
                        operation_info,
                        "progress/job_statistics/codec/cpu/decode/{}/$/completed/map/sum".format(codec)
                    )
                stats[codec]["codec/cpu/decode"] = cpu_read
        client.abort_transaction(tx)
    return stats


def run_compression_benchmarks(table, format, time_limit_sec, max_operations, sample_size, all_codecs, client=None):
    stats = run(table, time_limit_sec, max_operations, sample_size, all_codecs, client)

    stats_list = get_stats_list(stats)
    if format == "json":
        print(json.dumps(stats_list, sort_keys=True, indent=4))
    else:
        assert format == "csv"
        writer = csv.DictWriter(
            sys.stdout,
            fieldnames=["codec", "compression_ratio", "codec/cpu/encode", "codec/cpu/decode"]
        )
        writer.writeheader()
        for stat in stats_list:
            writer.writerow(stat)
