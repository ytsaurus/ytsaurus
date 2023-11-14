from yt.common import update

import yt.wrapper as yt

import os
import tempfile
import time

VALUES_COUNT = 10
SOURCE_DATA = [{"x": i} for i in range(VALUES_COUNT)]
MAP_RULES = {i: i**2 for i in range(VALUES_COUNT)}
MAPPED_DATA = [{"x": MAP_RULES[row["x"]]} for row in SOURCE_DATA]

MAPPER = """#!/usr/bin/env python3

import sys
import json


def load_mappings(path):
    with open(path, "r") as f:
        data = f.read()
    map_rules = {}
    for line in data.split("\\n"):
        line = line.strip()
        if line:
            key, value = map(int, line.split(":"))
            map_rules[key] = value
    return map_rules


def main():
    mappings_file = sys.argv[1]
    mappings = load_mappings(mappings_file)
    for line in sys.stdin:
        row = json.loads(line)
        if "$value" in row and row["$value"] is None:
            continue
        row["x"] = mappings[row["x"]]
        print(json.dumps(row))


if __name__ == "__main__":
    main()
"""


def dump_map_rules(rules):
    return "\n".join("{}:{}".format(key, value) for key, value in rules.items())


def run_check_impl(yt_client, logger, options, states, spec_patch=None, cloud=False):
    temp_path = options["temp_tables_path"]
    soft_map_timeout = options["soft_map_timeout"]

    check_result = states.UNAVAILABLE_STATE

    if not yt_client.exists(temp_path):
        logger.info('Creating "%s".', temp_path)
        yt_client.mkdir(temp_path, recursive=True)

    source_table_path = yt_client.create_temp_table(path=temp_path)
    logger.info("Created %s table.", source_table_path)
    mapped_table_path = yt_client.create_temp_table(path=temp_path)
    logger.info("Created %s table.", mapped_table_path)

    try:
        start_time = int(time.time())
        table_reader = {
            "workload_descriptor": {
                "category": "user_interactive",
            },
        }
        table_writer = {
            "enable_early_finish": True,
            "upload_replication_factor": 3,
            "min_upload_replication_factor": 2,
            "workload_descriptor": {
                "category": "user_interactive",
            },
            "sync_on_close": False,
        }
        yt_client.write_table(source_table_path, SOURCE_DATA, table_writer=table_writer)

        with tempfile.NamedTemporaryFile("w") as map_rules_file, tempfile.NamedTemporaryFile("w") as mapper_file:
            map_rules_file.write(dump_map_rules(MAP_RULES))
            map_rules_file.flush()

            mapper_file.write(MAPPER)
            mapper_file.flush()

            map_rules_file_name = os.path.basename(map_rules_file.name)
            spec = {
                "time_limit": 60 * 1000 * 3,
                "max_failed_job_count": 1,
                "job_io": {
                    "table_reader": table_reader,
                    "table_writer": table_writer,
                },
                "enable_legacy_live_preview": True,
            }
            if cloud:
                spec["pool_trees"] = options.get("pool_config", {}).get(options["cluster_name"], ["cloud"])
            if spec_patch is not None:
                spec = update(spec, spec_patch)

            operation = yt_client.run_map(
                "./mapper.py {}".format(map_rules_file_name),
                source_table_path,
                mapped_table_path,
                input_format="json",
                output_format="json",
                local_files=[
                    yt.LocalFile(mapper_file.name, file_name="mapper.py", attributes={"executable": True}),
                    map_rules_file.name,
                ],
                spec=spec,
                sync=False)

        logger.info("Map operation id '%s'", operation.id)

        try:
            operation.wait(print_progress=False)
        except yt.YtOperationFailedError as error:
            logger.error("Map operation '%s' failed", operation.id)
            logger.exception(error)
            return

        result = list(yt_client.read_table(mapped_table_path))
        if result == MAPPED_DATA:
            now = int(time.time())
            if now - start_time > soft_map_timeout:
                logger.warning("Table is mapped correctly but operation took "
                               "more than %d seconds", soft_map_timeout)
                check_result = states.PARTIALLY_AVAILABLE_STATE
            else:
                logger.info("Table is mapped correctly")
                check_result = states.FULLY_AVAILABLE_STATE
        else:
            logger.error("Table is mapped incorrectly; expected:\n%s\nreceived:\n%s", MAPPED_DATA, result)
    finally:
        yt_client.remove(source_table_path, force=True)
        yt_client.remove(mapped_table_path, force=True)

    return check_result
