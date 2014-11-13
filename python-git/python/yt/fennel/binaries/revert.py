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

def convert_from(item):
    cluster_name = item.pop("cluster_name", None)
    if cluster_name == "plato":
        for field_name in LOGBROKER_FIELDS:
            del item[field_name]

        yield fennel._untransform_record(fennel.convert_from_parsed(item))


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.stderr.write("Usage: {0} table_input table_output\n".format(sys.argv[0]));
    else:
        table_input = sys.argv[1]
        table_output = sys.argv[2]
        yt.config.format.TABULAR_DATA_FORMAT = yt.JsonFormat(process_table_index=True)
        yt.run_map(convert_from,
                   table_input,
                   table_output)
