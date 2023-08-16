from .logger import logger
from .helpers import is_table_empty

import yt.wrapper as yt

class AggregateReducer:
    def __init__(self, schema, aggregate, update):
        self.schema = schema
        self.aggregate = aggregate
        self.update = update
        self.aggregates = {}
        if aggregate:
            for c in schema.get_data_columns():
                if c.aggregate:
                    self.aggregates[c.name] = c
    def __call__(self, key, records):
        records = list(records)
        records = sorted(records, key=lambda x: x["@table_index"])
        # Check whether row should be deleted
        if records[-1]["@table_index"] == 2:
            return
        record = dict(key)
        for c in self.schema.get_data_column_names():
            record[c] = None
        for r in records:
            for c in self.schema.get_data_column_names():
                if c not in r.keys() and self.update == False:
                    r[c] = None
                if c in r.keys():
                    if c in self.aggregates.keys():
                        record[c] = self.aggregates[c].do_aggregate(record[c], r[c])
                    elif c in self.schema.get_column_names():
                        record[c] = r[c]
        yield record

def aggregate_data(schema, data, new_data, deletions, dst, aggregate, update):
    """
    Merges rows from |new_data| to |data| applying corresponding |aggregate|
    and |update| policies. Keys from |deletions| are deleted.
    Result is saved to |dst|.
    """

    logger.info("Aggregate data")

    key_columns = schema.get_key_column_names()

    input_tables = [data, new_data]
    if not is_table_empty(deletions):
        input_tables.append(deletions)
    yt.run_reduce(
        AggregateReducer(schema, aggregate, update),
        input_tables,
        yt.TablePath(
            dst,
            schema=schema.yson_with_unique()),
        reduce_by=key_columns,
        format=yt.YsonFormat(control_attributes_mode="row_fields"),
        spec={"title": "Aggregate data"})
    assert yt.get(dst + "/@sorted")
