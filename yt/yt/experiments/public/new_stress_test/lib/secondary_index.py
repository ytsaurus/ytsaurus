from .logger import logger
from .schema import Schema, Column, TInt64, ComparableYsonEntity
from .helpers import run_operation_and_wrap_error, sync_flush_table
from .verify import verify_output

import yt.wrapper as yt
import yt.yson as yson

import copy
import random


SYSTEM_EMPTY_COLUMN_NAME = "$empty"
ROW_LIMIT = 20000000


class FilterMapper:
    def __init__(self, index_table_schema):
        self.index_columns = [
            column["name"]
            for column in index_table_schema
            if column["name"] != SYSTEM_EMPTY_COLUMN_NAME]

    def __call__(self, row: dict):
        yield {key: row.get(key, None) for key in self.index_columns}


def make_random_secondary_index_schema(table_schema: Schema):
    index_key_columns = []
    index_value_columns = []
    for column in table_schema.get_data_columns():
        if column.aggregate:
            continue
        if random.random() < 0.33 and column.type.comparable():
            index_key_column = copy.deepcopy(column)
            index_key_column.sort_order = "ascending"
            index_key_columns.append(index_key_column)
            continue
        if random.random() < 0.5:
            index_value_columns.append(column)

    if len(index_value_columns) == 0:
        index_value_columns.append(Column(TInt64(), SYSTEM_EMPTY_COLUMN_NAME))

    index_key_columns += table_schema.get_key_columns()
    random.shuffle(index_key_columns)

    return Schema(index_key_columns, index_value_columns)

def verify_insert_secondary_index(
    table: str,
    schema: Schema,
    index_table: str,
    index_table_schema: Schema,
    dump_table: str,
    mismatch_table: str,
):
    logger.info("Secondary index insert verification: start")

    sync_flush_table(table)
    sync_flush_table(index_table)

    index_columns = {col.name for col in index_table_schema.columns}

    validated_schema = Schema(
        schema.key_columns,
        [col for col in schema.data_columns if col.name in index_columns])

    op = yt.run_merge(
        index_table,
        dump_table,
        mode="unordered",
        sync=False)
    run_operation_and_wrap_error(op, "Secondary index insert verification: dump index table")

    verify_output(
        validated_schema,
        table,
        dump_table,
        mismatch_table,
        "secondary index insert verification")

    logger.info("Secondary index insert verification: OK")

def make_in_condition(column: Column, sample: list):
    sample = [s[column.name] for s in sample]
    sample += [column.type.random() for _ in range(5)]
    return f"{column.name} IN (" + ", ".join([yson.dumps(s).decode() for s in sample]) + ")"

def make_range_condition(column: Column, sample: list):
    random.shuffle(sample)

    a = sample[0][column.name] if len(sample) > 0 else column.type.random()
    b = sample[1][column.name] if len(sample) > 1 else column.type.random()

    def comparable(x):
        return ComparableYsonEntity() if x is None else x

    a, b = (b, a) if comparable(b) < comparable(a) else (a, b)

    return column.name + " BETWEEN " + yson.dumps(a).decode() + " AND " + yson.dumps(b).decode()

def verify_select_secondary_index(
    table: str,
    schema: Schema,
    index_table: str,
    key_index_table_column: Column,
    mismatch_table: str
):
    """
    Generates random predicates on the first index key column.
    Compares the results of queries with index and without.
    """
    logger.info("Secondary index select verification: start")

    # read_table with random sampling does not work for dynamic tables
    sample = list(yt.select_rows(f"{key_index_table_column.name} FROM `{table}` limit 10"))

    if key_index_table_column.type.str() == "boolean":
        condition = random.choice(("", "not ")) + key_index_table_column.name
    elif random.random() < 0.5:
        condition = make_in_condition(key_index_table_column, sample)
    else:
        condition = make_range_condition(key_index_table_column, sample)

    logger.info(f"Secondary index select verification: generated condition {condition}")

    query_start = ", ".join(schema.get_column_names()) + f" FROM `{table}` "
    query_end = f"WHERE {condition} ORDER BY " + ", ".join(schema.get_key_column_names()) + " LIMIT 100000 "

    actual = list(yt.select_rows(
        query_start + f"WITH INDEX `{index_table}`" + query_end,
        input_row_limit=ROW_LIMIT,
        output_row_limit=ROW_LIMIT))
    expected = list(yt.select_rows(
        query_start + query_end,
        input_row_limit=ROW_LIMIT,
        output_row_limit=ROW_LIMIT))

    if actual != expected:
        yt.write_table(mismatch_table + ".fullscan", expected)
        yt.write_table(mismatch_table + ".with_index", actual)

        yt.set(mismatch_table + ".fullscan" + "/@condition", condition)
        yt.set(mismatch_table + ".with_index" + "/@condition", condition)

        raise Exception("Secondary index select verification: failed")

    logger.info("Secondary index select verification: OK")
