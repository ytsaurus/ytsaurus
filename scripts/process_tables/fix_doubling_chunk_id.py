import yt.wrapper as yt

import sys
import argparse

#import json

#table = "//crypta/production/storage/storage/metrics/2015-01-26"

def main():
    parser = argparse.ArgumentParser(description="Remove doubling chunks in table")
    parser.add_argument("table")
    args = parser.parse_args()

    print >>sys.stderr, "Fetching chunk row counts"
    table = args.table
    chunk_ids = yt.get(table + "/@chunk_ids")
    row_counts = [0]
    for id in chunk_ids:
        row_counts.append(row_counts[-1] + yt.get("#{}/@row_count".format(id)))

    lower_row_indexes = []
    upper_row_indexes = []
    double_chunk_indexes = []
    for i, id in enumerate(chunk_ids[:-1]):
        if id == chunk_ids[i + 1]:
            double_chunk_indexes.append(i)
            lower_row_indexes.append(row_counts[i])
            upper_row_indexes.append(row_counts[i + 1])
            print >>sys.stderr, "Doubled chunk:", double_chunk_indexes[-1], lower_row_indexes[-1], upper_row_indexes[-1]

    if double_chunk_indexes:
        print >>sys.stderr, "Found %d doubled chunks" % len(double_chunk_indexes)
        table_paths = []
        for i in xrange(len(lower_row_indexes) + 1):
            table_path = yt.TablePath(table)
            if i > 0:
                table_path.attributes["lower_limit"] = {"row_index" : upper_row_indexes[i - 1]}
            if i < len(double_chunk_indexes):
                table_path.attributes["upper_limit"] = {"row_index" : lower_row_indexes[i]}
            table_paths.append(table_path)
        
        print >>sys.stderr, "Running merge from %s to %s" % (table, table + "_fixed")
        yt.run_merge(table_paths, table + "_fixed")

        assert yt.get(table + "_fixed/@chunk_count") == len(chunk_ids) - len(double_chunk_indexes)

        yt.run_merge(table + "_fixed", table)
        yt.remove(table + "_fixed")

if __name__ == "__main__":
    main()


