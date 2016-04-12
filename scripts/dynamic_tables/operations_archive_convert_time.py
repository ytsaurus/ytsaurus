#!/usr/bin/env python

import yt.tools.dynamic_tables as dynamic_tables
import yt.tools.operations_archive as operations_archive

def mapper_by_id(row):
    row['start_time'] = row['start_time'] * 1000000
    row['finish_time'] = row['finish_time'] * 1000000
    yield row

def mapper_by_start_time(row):
    row['start_time'] = row['start_time'] * 1000000
    yield row

def main():
    new_by_start_time_path = operations_archive.BY_START_TIME_ARCHIVE + ".fixts"
    operations_archive.create_ordered_by_start_time_table(new_by_start_time_path);
    dynamic_tables.run_convert(mapper_by_start_time, operations_archive.BY_START_TIME_ARCHIVE, new_by_start_time_path)

    new_by_id_path = operations_archive.BY_ID_ARCHIVE + ".fixts"
    operations_archive.create_ordered_by_id_table(new_by_id_path);
    dynamic_tables.run_convert(mapper_by_id, operations_archive.BY_ID_ARCHIVE, new_by_id_path)

if __name__ == "__main__":
    main()

