#!/usr/bin/python

import yt.wrapper as yt

import sys


def transform_row(row):
    if isinstance(row, dict):
        result = []
        for key, value in sorted(row.iteritems()):
            result.append((key, transform_row(value)))
        return ("map", tuple(result))
    elif isinstance(row, list):
        result = []
        for value in row:
            result.append(transform_row(value))
        return ("list", tuple(result))
    else:
        return ("value", row)

def untransform(row):
    if not isinstance(row, tuple):
        raise RuntimeError()

    if len(row) != 2:
        raise RuntimeError()

    type_, v = row
    if type_ == "value":
        return v
    elif type_ == "map":
        result = {}
        for key, value in v:
            result[key] = untransform(value)
        return result
    elif type_ == "list":
        result = []
        for value in v:
            result.append(untransform(value))
        return result
    else:
        assert False, row


def diff(key, rows):
    before = set()
    after = set()
    for row in rows:
        table_index = row.pop("table_index", None)

        if table_index == 0:
            before.add(transform_row(row))
        elif table_index == 1:
            after.add(transform_row(row))
        else:
            assert False, table_index

    added = after.difference(before)
    removed = before.difference(after)
    for row in added:
        try:
            row = untransform(row)
            row["added"] = "true"
            yield row
        except RuntimeError:
            sys.stderr.write("Bad row: " + str(row) + "\n")
            raise
    for row in removed:
        try:
            row = untransform(row)
            row["removed"] = "true"
            yield row
        except RuntimeError:
            sys.stderr.write("Bad row: " + str(row) + "\n")
            raise


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.stderr.write("Usage: {0} before after table_output\n".format(sys.argv[0]));
    else:
        before, after, output = sys.argv[1:4]
        sorted_by = yt.get(before + "/@sorted_by")
        yt.config.format.TABULAR_DATA_FORMAT = yt.JsonFormat(process_table_index=True)
        yt.run_reduce(diff,
                      source_table=[before, after],
                      destination_table=output,
                      reduce_by=sorted_by,
                      spec={"max_failed_job_count": 1})
