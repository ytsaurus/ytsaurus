from .common import forbidden_inside_job
from .run_operation_commands import run_map_reduce
from .cypress_commands import get
from .ypath import TablePath
from .format import SkiffFormat


@forbidden_inside_job
def shuffle_table(table, sync=True, temp_column_name="__random_number", spec=None, client=None):
    """Shuffles table randomly.

    :param table: table.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param temp_column_name: temporary column name which will be used in reduce_by.
    :type temp_column_name: str.

    . seealso::  :ref:`operation_parameters`.
    """

    shuffle_mapper = """python3 -c '
import sys, struct, random
i = sys.stdin.buffer
o = sys.stdout.buffer
while(not i.closed):
  hdr = i.read(6)
  if not hdr:
    break
  ti, yson_len = struct.unpack_from("<HI", hdr, 0)
  o.write(struct.pack("<HdI", ti, random.random(), yson_len))
  d = i.read(yson_len)
  if len(d) != yson_len:
    raise RuntimeError()
  o.write(d)'
"""

    shuffle_reducer = """python3 -c '
import sys, struct
i = sys.stdin.buffer
o = sys.stdout.buffer
while(not i.closed):
  hdr = i.read(14)
  if not hdr:
    break
  ti, rnd, yson_len = struct.unpack_from("<HdI", hdr, 0)
  o.write(struct.pack("<HI", ti, yson_len))
  d = i.read(yson_len)
  if len(d) != yson_len:
    raise RuntimeError()
  o.write(d)'
"""

    mapper_input_format = SkiffFormat([{"wire_type": "tuple", "children": [{"name": "$other_columns", "wire_type": "yson32"}]}])
    mapper_output_format = SkiffFormat([{"wire_type": "tuple", "children": [{"name": temp_column_name, "wire_type": "double"}, {"name": "$other_columns", "wire_type": "yson32"}]}])

    reducer_input_format = mapper_output_format
    reducer_output_format = mapper_input_format

    schema = None
    attributes = get(table + "/@", attributes=["schema", "schema_mode"], client=client)
    if attributes.get("schema_mode") == "strong":
        schema = attributes["schema"]
        for elem in schema:
            if "sort_order" in elem:
                del elem["sort_order"]

    return run_map_reduce(shuffle_mapper, shuffle_reducer, table, TablePath(table, schema=schema, client=client),
                          map_input_format=mapper_input_format, map_output_format=mapper_output_format,
                          reduce_input_format=reducer_input_format, reduce_output_format=reducer_output_format,
                          reduce_by=temp_column_name, sync=sync, spec=spec, client=client)
