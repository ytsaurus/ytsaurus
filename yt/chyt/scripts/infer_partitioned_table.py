#!/usr/bin/python3

import yt.wrapper as yt
import yt.yson as yson

import argparse
import pytz
import datetime
import logging
import collections
from typing import List, Dict

MST = pytz.timezone("Europe/Moscow")

SCALES = ["1d", "30min"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s  %(levelname).1s  %(module)s:%(lineno)d  %(message)s"))
logger.addHandler(handler)


def get_scale_interval(scale: str):
    if scale == "30min":
        return datetime.timedelta(minutes=30)
    elif scale == "1d":
        return datetime.timedelta(days=1)
    else:
        raise ValueError("Invalid scale {}".format(scale))


def upscale(partition_dt: datetime.datetime, scale: str):
    if scale == "30min":
        return partition_dt
    elif scale == "1d":
        # Strip everything except date.
        return datetime.datetime.combine(partition_dt.date(), datetime.time())
    else:
        raise ValueError("Invalid scale {}".format(scale))


def parse_datetime(partition_key: str, scale: str):
    if scale == "30min":
        dt = datetime.datetime.fromisoformat(partition_key)
        return dt.astimezone(MST)
    elif scale == "1d":
        date = datetime.date.fromisoformat(partition_key)
        dt = datetime.datetime.combine(date, datetime.time())
        return dt.astimezone(MST)
    else:
        raise ValueError("Invalid scale {}".format(scale))


class HashableSchema:
    def __init__(self, schema):
        schema.sort(key=lambda column: column["name"])
        self.yson = yson.dumps(schema)
        self.schema = schema

    def __eq__(self, other):
        return self.yson == other.yson

    def __hash__(self):
        return hash(self.yson)

    def __str__(self):
        return str(self.schema)


class Partition:
    def __init__(self, path: str, key: str, dt: datetime.datetime, schema: dict, scale: str):
        self.path = path
        self.dt = dt
        self.key = key
        self.schema = HashableSchema(schema)
        self.scale = scale
        self.children = []

    def start_dt(self):
        return self.dt

    def finish_dt(self):
        return self.dt + get_scale_interval(self.scale)

    def __str__(self):
        return self.key


def collect_partitions(log_path: str):
    for scale in SCALES:
        scale_path = log_path + "/" + scale
        if not yt.exists(scale_path):
            logger.info("Scale %s does not exist", scale)
            return
        logger.info("Processing scale %s", scale)
        nodes = yt.list(scale_path, attributes=["schema", "key", "type", "path"])
        for node in nodes:
            if node.attributes["type"] != "table":
                pass
            dt = parse_datetime(node.attributes["key"], scale)
            yield Partition(node.attributes["path"], node.attributes["key"], dt, node.attributes["schema"], scale)


def distribute_by_scales(partitions: List[Partition]) -> Dict[str, List[Partition]]:
    scale_to_partitions = collections.defaultdict(list)
    for partition in partitions:
        scale_to_partitions[partition.scale].append(partition)
    return scale_to_partitions


def build_covering(partitions: List[Partition]):
    scale_to_partitions = distribute_by_scales(partitions)

    last_partition = None

    for scale, scale_partitions in scale_to_partitions.items():
        scale_partitions.sort(key=lambda partition: partition.start_dt())

    child_index = 0

    full_child_count = get_scale_interval("1d") // get_scale_interval("30min")

    for partition in scale_to_partitions["1d"]:
        while child_index < len(scale_to_partitions["30min"]):
            child_partition = scale_to_partitions["30min"][child_index]
            if child_partition.finish_dt() > partition.finish_dt():
                break

            if child_partition.start_dt() >= partition.start_dt():
                partition.children.append(child_partition)
            else:
                yield child_partition

            child_index += 1

        if len(partition.children) == full_child_count:
            yield from partition.children
        else:
            yield partition

    while child_index < len(scale_to_partitions["30min"]):
        child_partition = scale_to_partitions["30min"][child_index]
        yield child_partition
        child_index += 1


class CommonSchemaInferrer:
    class ColumnState:
        def __init__(self, name):
            self.name = name
            self.type_values = set()
            self.required_values = set()

            self.type = None
            self.required = None
            self.sort_order = None

            self.index = 10**9  # Infinity.

        def _transform_to_any(self, reason):
            logger.info("Transforming column %s to any because of following reason: %s", self.name, reason)
            self.type = "any"

        def _transform_to_optional(self, reason):
            logger.info("Transforming column %s to optional because of following reason: %s", self.name, reason)
            self.required = False

        def account_occurrence(self, column_schema: dict):
            self.type_values.add(column_schema["type"])
            self.required_values.add(column_schema["required"])
            if column_schema.get("sort_order") != "ascending":
                self.sort_order = None

        def account_absence(self, strict: bool):
            self.sort_order = None
            self._transform_to_optional("column is absent in table")
            if not strict:
                # This column may have arbitrary type in this table.
                self._transform_to_any("column is absent in non-strict table")

        def flush_schema(self):
            if len(self.type_values) >= 2:
                self._transform_to_any(
                    "column occurs with different types: {}".format(self.type_values))
            else:
                assert len(self.type_values) == 1
                if self.type is None:
                    self.type = self.type_values.pop()

            if len(self.required_values) >= 2:
                assert len(self.required_values) == 2
                self._transform_to_optional("column changes requiredness")
            else:
                assert len(self.required_values) == 1
                if self.required is None:
                    self.required = self.required_values.pop()

            return {"name": self.name, "type": self.type, "required": self.required, "sort_order": self.sort_order}

    def __init__(self, columns):
        self.column_to_state = dict()
        for column in columns:
            self.column_to_state[column] = self.ColumnState(column)
        self.strict = True
        self.key_columns = None

    @staticmethod
    def _get_key_columns(schema):
        result = []
        for column_schema in schema:
            if column_schema.get("sort_order") == "ascending":
                result.append(column_schema["name"])
            else:
                return result

    def add_schema(self, schema, strict):
        column_to_column_schema = dict()
        for column_schema in schema:
            column_to_column_schema[column_schema["name"]] = column_schema
            assert column_schema["name"] in self.column_to_state

        for column, state in self.column_to_state.items():
            column_schema = column_to_column_schema.get(column)
            if column_schema is not None:
                state.account_occurrence(column_schema)
            else:
                state.account_absence(strict)

        self.strict = self.strict and strict

        key_columns = self._get_key_columns(schema)
        if self.key_columns is None:
            self.key_columns = key_columns
        else:
            mismatch_index = 0
            while (mismatch_index < len(self.key_columns) and mismatch_index < len(key_columns) and
                    self.key_columns[mismatch_index] == key_columns[mismatch_index]):
                mismatch_index += 1
            self.key_columns = self.key_columns[:mismatch_index]

    def flush_schema(self):
        for i, column in enumerate(self.key_columns):
            self.column_to_state[column].index = i
        states = list(self.column_to_state.values())
        states.sort(key=lambda state: state.index)
        schema = [state.flush_schema() for state in states]

        return yson.to_yson_type(schema, attributes={"strict": self.strict})


def infer_common_schema(partitions: List[Partition]):
    if len(partitions) == 0:
        raise ValueError("Empty list of partitions")
    schemas = set()
    for partition in partitions:
        schemas.add(partition.schema)
    if len(schemas) == 1:
        logger.info("All partitions share same schema")
        return schemas.pop().schema
    logger.info("There are %d distinct schemas", len(schemas))
    for schema in schemas:
        logger.debug("Schema: %s", schema)

    columns = set()
    for schema in schemas:
        for column_schema in schema:
            columns.add(column_schema["name"])

    inferrer = CommonSchemaInferrer(columns)
    for schema in schemas:
        inferrer.add_schema(schema.schema, schema.schema.attributes.get("strict", True))
    schema = inferrer.flush_schema()
    logger.debug("Inferred common schema: %s", schema)

    return schema

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--log-path", help="Path to logfeller log root (containing 1d and/or 30m directories)",
                        required=True)
    parser.add_argument("--name", default="pt", help="Name of partitioned table to create")
    parser.add_argument("--force", action="store_true", help="Override partitioned table if it already exists")
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    partitions = list(collect_partitions(args.log_path))
    covering = list(build_covering(partitions))

    logger.info("Built covering consisting of %d partitions", len(covering))
    for partition in covering:
        logger.debug("Partition: %s", partition)

    schema = infer_common_schema(covering)

    partitioned_by = ["partition_key"]
    schema.insert(0, {"name": "partition_key", "type": "string", "sort_order": "ascending", "required": True})
    partition_configs = [{
        "path": partition.path,
        "key": [partition.key],
    } for partition in partitions]


    yt.create("map_node", args.log_path + "/" + args.name, force=args.force, attributes={
        "schema": schema,
        "partitioned_by": partitioned_by,
        "partitions": partition_configs,
        "assume_partitioned_table": True,
    })


if __name__ == "__main__":
    main()
