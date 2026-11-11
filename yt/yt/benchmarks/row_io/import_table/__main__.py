#!/usr/bin/env python3
"""Export a YT table into an offline dataset for the row_io benchmark."""

import yt.wrapper as yt
import yt.yson as yson

import argparse
import copy
import os
from pathlib import Path
import shutil
import sys
import tempfile


MAX_JSON_INTEGER = 1 << 53


def clamp_integer_values(value):
    attributes = getattr(value, "attributes", None)
    if isinstance(value, dict):
        for key, item in value.items():
            value[key] = clamp_integer_values(item)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            value[index] = clamp_integer_values(item)
    elif isinstance(value, int) and not isinstance(value, (bool, yson.YsonBoolean)):
        lower_bound = 0 if isinstance(value, yson.YsonUint64) else -MAX_JSON_INTEGER
        value = type(value)(max(lower_bound, min(int(value), MAX_JSON_INTEGER)))
        if attributes is not None:
            value.attributes = attributes
    if attributes:
        for key, item in attributes.items():
            attributes[key] = clamp_integer_values(item)
    return value


def column_name(name):
    return name.decode("utf-8") if isinstance(name, bytes) else str(name)


def benchmark_schema(source, columns, yson):
    """Keep logical types, but not cluster-only computed/hunk/storage settings."""
    selected = None if columns is None else {column_name(name) for name in columns}
    result = []
    key_names = set()
    for column in source:
        name = column_name(column["name"])
        if "sort_order" in column:
            key_names.add(name)
        if selected is not None and name not in selected:
            continue
        target = {"name": name}
        if "type_v3" in column:
            target["type_v3"] = copy.deepcopy(column["type_v3"])
        else:
            target["type"] = column["type"]
            if "required" in column:
                target["required"] = column["required"]
        if "sort_order" in column:
            target["sort_order"] = column["sort_order"]
        result.append(target)
    keep_keys = selected is None or key_names <= selected
    if not keep_keys:
        for column in result:
            column.pop("sort_order", None)
    schema = yson.to_yson_type(result, attributes={
        "strict": True,
        "unique_keys": bool(source.attributes.get("unique_keys", False)) and keep_keys,
    })
    return schema


def write_rows(client, path, schema, output, yt, yson, clamp_integers=False):
    names = {column_name(column["name"]) for column in schema}
    row_count = 0
    rows = client.read_table(
        path,
        format=yt.YsonFormat(
            format="binary", encoding=None, control_attributes_mode="none",
            attributes={"complex_type_mode": "named"}),
        raw=False,
        unordered=False,
        enable_read_parallel=False,
    )
    try:
        for row in rows:
            for name in row:
                name = column_name(name)
                if name not in names:
                    schema.append({"name": name, "type": "any"})
                    names.add(name)
            # TODO(coteeq): Deal with key uniqueness/ordering issues
            if clamp_integers:
                row = clamp_integer_values(row)
            payload = yson.dumps(row, yson_format="binary") + b";\n"
            output.write(payload)
            row_count += 1
    finally:
        if hasattr(rows, "close"):
            rows.close()
    if row_count == 0:
        raise ValueError("The selected table/range is empty; import at least one row")
    if not schema:
        raise ValueError("The selected data has no columns")
    return row_count


def export_dataset(client, path, destination, yt, yson, clamp_integers=False):
    destination = Path(destination).expanduser().absolute()
    if destination.exists():
        raise FileExistsError(f"Refusing to overwrite {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
    try:
        # Pin both schema and rows while exporting a static table.
        with client.Transaction(attributes={"title": "row_io benchmark export"}):
            client.lock(str(path), mode="snapshot")
            source_schema = client.get(str(path) + "/@schema")
            schema = benchmark_schema(source_schema, path.attributes.get("columns"), yson)
            with (temporary / "rows.yson").open("wb") as output:
                row_count = write_rows(
                    client, path, schema, output, yt, yson, clamp_integers=clamp_integers)
        schema_payload = yson.dumps(schema, yson_format="binary") + b"\n"
        (temporary / "schema.yson").write_bytes(schema_payload)
        os.rename(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary)
        raise
    return row_count


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("table", help="Table path; rich paths/ranges are accepted")
    parser.add_argument("output", type=Path, help="New local dataset directory")
    parser.add_argument(
        "--proxy", default=os.environ.get("YT_PROXY"), help="Cluster proxy (or YT_PROXY)")
    parser.add_argument(
        "--clamp-integers", action="store_true",
        help="Clamp int64 to [-2^53, 2^53] and uint64 to [0, 2^53], recursively")
    args = parser.parse_args(argv)
    if not args.proxy:
        parser.error("Specify --proxy or YT_PROXY")
    return args


def main(argv=None):
    args = parse_arguments(argv)
    client = yt.YtClient(proxy=args.proxy)
    path = yt.TablePath(args.table, client=client)
    row_count = export_dataset(
        client, path, args.output, yt, yson, clamp_integers=args.clamp_integers)
    print(f"Exported {row_count} rows to {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Import failed: {error}", file=sys.stderr)
        sys.exit(1)
