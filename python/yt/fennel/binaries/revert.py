#!/usr/bin/python

import yt.wrapper as yt
from yt.fennel import fennel

import sys

LOGBROKER_FIELDS = [
    "_stbx",
    "iso_eventtime",
    "source_uri",
    "subkey"
]

class ConvertFrom(object):
    def __init__(self, cluster_name):
        self._cluster_name = cluster_name

    def __call__(self, item):
        cluster_name = item.pop("cluster_name", None)
        if cluster_name == self._cluster_name:
            for field_name in LOGBROKER_FIELDS:
                del item[field_name]
            yield fennel._untransform_record(fennel.convert_from_parsed(item))


if __name__ == "__main__":
    if len(sys.argv) < 4:
        sys.stderr.write("Usage: {0} table_input table_output cluster_name\n".format(sys.argv[0]));
    else:
        table_input, table_output, cluster_name = sys.argv[1:4]
        yt.config.format.TABULAR_DATA_FORMAT = yt.JsonFormat(process_table_index=True)
        yt.run_map(ConvertFrom(cluster_name),
                   table_input,
                   table_output)
