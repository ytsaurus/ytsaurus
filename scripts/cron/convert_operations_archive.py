#!/usr/bin/python

import sys
import yt.wrapper as yt
import yt.yson as yson
from clear_operations import get_filter_factors

OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive/ordered_by_id"

yt.config.VERSION = "v3"

def main():
    archive = yson.load(yt.select_rows("* from [{}]".format(OPERATIONS_ARCHIVE_PATH), format=yt.YsonFormat()), "list_fragment")

    for item in archive:
        row = {
            "id": item.id,
            "filter_factors": get_filter_factors(item.id, item)
            }
        yt.insert_rows(OPERATIONS_ARCHIVE_PATH, [row])

if __name__ == "__main__":
    main()
