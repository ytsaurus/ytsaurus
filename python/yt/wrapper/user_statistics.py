import yt.yson as yson

import os
import sys

def write_statistics(dict):
    try:
        # File descriptor 5 is reserved for user statistics
        os.write(5, yson.dumps([dict], yson_type="list_fragment"))
    except OSError:
        sys.stderr.write("Failed to write user statistics")

