from .common import forbidden_inside_job
from .run_operation_commands import run_map_reduce

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

    return run_map_reduce(shuffle_mapper, shuffle_reducer, table, table, reduce_by=temp_column_name, sync=sync, spec=spec, client=client)
