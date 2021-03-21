import yt.wrapper as yt
import fnmatch
import os.path
import datetime

def find(pattern):
    return yt.search("//sys/clickhouse/kolkhoz", path_filter=lambda path: fnmatch.fnmatch(os.path.basename(path), pattern), attributes=["modification_time"])


def main():
    tables = list(find("core_table.*")) + list(find("stderr_table.*"))
    tables.sort(key=lambda t: t.attributes["modification_time"])
    min_timestamp = (datetime.datetime.now() - datetime.timedelta(days=7)).isoformat()
    tables = [table for table in tables if table.attributes["modification_time"] < min_timestamp]

    for table in tables:
        print table.attributes["modification_time"], table

    yt.batch_apply(yt.remove, tables)


if __name__ == "__main__":
    main()
