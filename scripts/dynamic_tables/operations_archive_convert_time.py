#!/usr/bin/env python

import yt.tools.dynamic_tables as dynamic_tables
import yt.tools.operations_archive as operations_archive
import yt.wrapper as yt

def mapper_by_id(row):
    row['start_time'] = row['start_time'] + 1000000 * 60 * 60 * 3
    row['finish_time'] = row['finish_time'] + 1000000 * 60 * 60 * 3
    yield row

def mapper_by_start_time(row):
    row['start_time'] = row['start_time'] + 1000000 * 60 * 60 * 3
    yield row

def run_convert(mapper, target_table, dst_table):
    dynamic_tables.mount_table(target_table)
    client = dynamic_tables.DynamicTablesClient(job_count = 1000, job_memory_limit=4 * 2**30, max_failed_job_count=1000)
    client.run_map_dynamic(mapper, target_table, dst_table, batch_size=10000)
    dynamic_tables.unmount_table(dst_table)
    yt.set(dst_table + "/@forced_compaction_revision", yt.get(dst_table + "/@revision"))
    dynamic_tables.mount_table(dst_table)

def swap_table(target, source):
    bak_path = target + ".bak"
    yt.move(target, bak_path)
    yt.move(source, target)

def main():
    new_by_start_time_path = operations_archive.BY_START_TIME_ARCHIVE + ".fixts"
    operations_archive.create_ordered_by_start_time_table(new_by_start_time_path);
    run_convert(mapper_by_start_time, operations_archive.BY_START_TIME_ARCHIVE, new_by_start_time_path)

    new_by_id_path = operations_archive.BY_ID_ARCHIVE + ".fixts"
    operations_archive.create_ordered_by_id_table(new_by_id_path);
    run_convert(mapper_by_id, operations_archive.BY_ID_ARCHIVE, new_by_id_path)

    dynamic_tables.unmount_table(operations_archive.BY_START_TIME_ARCHIVE)
    dynamic_tables.unmount_table(new_by_start_time_path)
    dynamic_tables.unmount_table(operations_archive.BY_ID_ARCHIVE)
    dynamic_tables.unmount_table(new_by_id_path)

    #with yt.Transaction():
    swap_table(operations_archive.BY_START_TIME_ARCHIVE, new_by_start_time_path)
    swap_table(operations_archive.BY_ID_ARCHIVE, new_by_id_path)
    yt.set_attribute(operations_archive.OPERATIONS_ARCHIVE_PATH, "version", 1)

    dynamic_tables.mount_table(operations_archive.BY_START_TIME_ARCHIVE)
    dynamic_tables.mount_table(operations_archive.BY_ID_ARCHIVE)

if __name__ == "__main__":
    main()

