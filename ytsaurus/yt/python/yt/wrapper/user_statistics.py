from . import yson

import os
import sys


def write_statistics(dict):
    """Writes user statistics to proper file descriptor.
    This function must be called from job.

    :param dict dict: python dictionary with statistics.
    """
    try:
        # File descriptor 5 is reserved for user statistics
        os.write(5, yson.dumps([dict], yson_type="list_fragment"))
    except OSError:
        sys.stderr.write("Failed to write user statistics\n")


def _get_field(path, name):
    data = open(path, "r").read()
    for line in data.split("\n"):
        if line.startswith(name):
            return int(line.split()[1])


def _get_value(path):
    return int(open(path, "r").read())
