#!/usr/bin/env python

import yt.tools.dynamic_tables as dynamic_tables
import yt.tools.operations_archive as operations_archive
import yt.yson as yson
import logging

def convert_key(row):
    parts = row["id"].split("-")

    id_hi = long(parts[3], 16) << 32 | int(parts[2], 16)
    id_lo = long(parts[1], 16) << 32 | int(parts[0], 16)

    return {
        'id_hi': yson.YsonUint64(id_hi),
        'id_lo': yson.YsonUint64(id_lo),
        'start_time': operations_archive.datestr_to_timestamp(row["start_time"])
    }

def mapper_by_id(row):
    result = convert_key(row)

    result['finish_time'] = operations_archive.datestr_to_timestamp(row["finish_time"])

    other_columns = [
        'authenticated_user',
        'brief_progress',
        'brief_spec',
        'filter_factors',
        'operation_type',
        'progress',
        'result',
        'spec',
        'state'
    ]

    for column in other_columns:
        result[column] = row[column]

    yield result

def mapper_by_start_time(row):
    result = convert_key(row)
    result['dummy'] = 0

    yield result

def main():
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    new_by_start_time_path = operations_archive.BY_START_TIME_ARCHIVE + ".xxx"
    operations_archive.create_ordered_by_start_time_table(new_by_start_time_path, force=True);
    dynamic_tables.run_convert(mapper_by_start_time, operations_archive.BY_START_TIME_ARCHIVE + ".bak", new_by_start_time_path)

    new_by_id_path = operations_archive.BY_ID_ARCHIVE + ".xxx"
    operations_archive.create_ordered_by_id_table(new_by_id_path, force=True);
    dynamic_tables.run_convert(mapper_by_id, operations_archive.BY_ID_ARCHIVE + ".bak", new_by_id_path)

if __name__ == "__main__":
    main()

