from yt.wrapper import YtClient, ypath_join

import argparse
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)-15s\t%(levelname)s\t%(message)s'))
logger.addHandler(handler)


def get_column_mapping(schema):
    return {column["name"]: column for column in schema}


class Patcher:
    def __init__(self, patch):
        self.patch = patch

    def __call__(self, row):
        for key in self.patch:
            row[key] = self.patch[key]
        yield row


def add_columns(client, path, new_schema, added_columns):
    new_column_mapping = get_column_mapping(new_schema)
    patch = {}
    for column_name in added_columns:
        column_type = new_column_mapping[column_name]["type"]
        if column_type in ("int64", "int32"):
            patch[column_name] = 0
        elif column_type in ("double",):
            patch[column_name] = 0.0
        else:
            raise RuntimeError("Adding column '{}' of type {} is not supported".format(column_name, column_type))

    if not client.get(path + "/@sorted"):
        logger.warning("Table %s is not sorted; skipping")
        return

    sorted_by = [column["name"] for column in new_schema if column.get("sort_order", "") == "ascending"]
    patcher = Patcher(patch)
    with client.TempTable() as temp_table:
        client.alter_table(temp_table, new_schema)
        client.run_map(patcher, path, client.TablePath(temp_table, sorted_by=sorted_by), ordered=True)
        client.move(temp_table, path, force=True)


def main():
    parser = argparse.ArgumentParser(description="Add columns for older tables")
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--logs-path", required=True)
    parser.add_argument("--dry-run", action="store_true", default=False)
    args = parser.parse_args()

    client = YtClient(args.cluster)
    items = client.list(args.logs_path, attributes=["schema", "type"])

    items = [item for item in items if item.attributes["type"] == "table"]
    items.sort()

    # yson.dump(items[-1].attributes["schema"], sys.stdout.buffer, yson_format="pretty")
    recent_schema = items[-1].attributes["schema"]
    recent_schema_column_mapping = get_column_mapping(recent_schema)
    for item in sorted(items):
        schema = item.attributes["schema"]
        name = str(item)

        column_mapping = get_column_mapping(schema)
        missing_columns = set(recent_schema_column_mapping.keys()) - set(column_mapping.keys())
        if missing_columns:
            logger.info("Adding new columns to table (name: %s, columns: %s)", name, missing_columns)
            if not args.dry_run:
                add_columns(
                    client,
                    ypath_join(args.logs_path, name),
                    new_schema=recent_schema,
                    added_columns=missing_columns,
                )


if __name__ == "__main__":
    main()
