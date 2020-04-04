import yt.wrapper as yt

import sys

def get_ranges(num, step):
    for i in xrange(1 + (num - 1) / step):
        yield (i * step, min((i + 1) * step, num))

def merge_large_table(table, destination=None):
    if destination is None:
        destination = table

    chunk_count = yt.get(table + "/@chunk_count")
    operations = []
    tables = []
    print >>sys.stderr, "Starting merge operations..."
    for start_index, end_index in get_ranges(chunk_count, 100000):
        table_path = yt.TablePath(table)
        table_path.attributes["lower_limit"] = {"chunk_index": start_index}
        table_path.attributes["upper_limit"] = {"chunk_index": end_index}
        dst = destination + "_" + str(len(operations))
        operations.append(yt.run_merge(table_path, dst, sync=False, spec={"combine_chunks":"true"}))
        tables.append(dst)

    print >>sys.stderr, "Waiting..."
    for op in operations:
        op.wait()

    row_count = sum([yt.get(t + "/@row_count") for t in tables])
    if row_count != yt.get(table + "/@row_count"):
        print >>sys.stderr, "Row count mismatch!", table, row_count, yt.get(table + "/@row_count")
        return

    yt.run_merge(tables, destination)
    for t in tables:
        yt.remove(t)


if __name__ == "__main__":
    merge_large_table(sys.argv[1])
