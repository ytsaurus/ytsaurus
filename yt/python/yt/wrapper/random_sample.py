from .config import get_config
from .common import forbidden_inside_job
from .cypress_commands import get, copy
from .errors import YtError
from .run_operation_commands import run_merge
from .shuffle import shuffle_table
from .table import TempTable
from .table_commands import read_table, write_table
from .ypath import TablePath

import random


def _sample_rows_from_table_in_mapreduce(table, output_table, total_table_row_count, row_count, client=None):
    with TempTable(client=client) as temp_table:
        sampling_rate = min(float(row_count) / total_table_row_count * 2, 1.0)

        copy(table, temp_table, client=client, force=True)

        spec = {"map_job_io": {"table_reader": {"sampling_rate": sampling_rate}}}
        shuffle_table(temp_table, spec=spec, client=client)

        run_merge(TablePath(temp_table, end_index=row_count), output_table, client=client)


def _sample_rows_from_table_generator(table, total_table_row_count, row_count, client=None):
    used_indexes = set()

    while len(used_indexes) != row_count:
        index = random.randint(0, total_table_row_count - 1)
        if index in used_indexes:
            continue

        used_indexes.add(index)
        iterator = read_table(TablePath(table, exact_index=index), client=client)
        row = next(iterator)
        yield row


@forbidden_inside_job
def sample_rows_from_table(table, output_table, row_count, client=None):
    """Samples random rows from table.

    :param table: path to input table
    :type table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param output_table: path to output table
    :type output_table: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    :param int row_count: the number of rows to be sampled

    """
    total_table_row_count = get(table + "/@row_count", client=client)

    table = TablePath(table)
    if "ranges" in table.attributes:
        raise YtError("Specifying ranges for sample_rows_from_table is not supported")

    if total_table_row_count < row_count:
        raise YtError("Total table row count is less than desired row count")

    if row_count > get_config(client)["max_row_count_for_local_sampling"]:
        return _sample_rows_from_table_in_mapreduce(table, output_table, total_table_row_count, row_count, client)

    rows_generator = _sample_rows_from_table_generator(table, total_table_row_count, row_count, client)
    write_table(output_table, rows_generator, client=client)
