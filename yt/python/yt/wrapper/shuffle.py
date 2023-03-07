from .common import forbidden_inside_job
from .run_operation_commands import run_map_reduce
from .cypress_commands import get
from .ypath import TablePath

@forbidden_inside_job
def shuffle_table(table, sync=True, temp_column_name="__random_number", spec=None, client=None):
    """Shuffles table randomly.

    :param table: table.
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param temp_column_name: temporary column name which will be used in reduce_by.
    :type temp_column_name: str.

    . seealso::  :ref:`operation_parameters`.
    """
    def shuffle_mapper(row):
        import random
        row[temp_column_name] = random.random()
        yield row

    def shuffle_reducer(_, rows):
        for row in rows:
            del row[temp_column_name]
            yield row

    schema = None
    attributes = get(table + "/@", attributes=["schema", "schema_mode"], client=client)
    if attributes.get("schema_mode") == "strong":
        schema = attributes["schema"]
        for elem in schema:
            if "sort_order" in elem:
                del elem["sort_order"]

    return run_map_reduce(shuffle_mapper, shuffle_reducer, table, TablePath(table, schema=schema, client=client),
                          reduce_by=temp_column_name, sync=sync, spec=spec, client=client)
