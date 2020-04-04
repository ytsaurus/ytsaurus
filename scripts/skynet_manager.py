import argparse

import yt.wrapper


HOME = "//home/yt-skynet-m5r"

REQUESTS_TABLE = HOME + "/requests"
REQUESTS_TABLE_SCHEMA = [
    {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(table_path)"},
    {"name": "table_path", "type": "string", "sort_order": "ascending"},
    {"name": "options", "type": "string", "sort_order": "ascending"},
    {"name": "state", "type": "string"},
    {"name": "update_time", "type": "uint64"},
    {"name": "owner_id", "type": "string"},
    {"name": "error", "type": "any"},
    {"name": "progress", "type": "any"},
    {"name": "resources", "type": "any"}
]

RESOURCES_TABLE = HOME + "/resources"
RESOURCES_TABLE_SCHEMA = [
    {"name": "resource_id", "type": "string", "sort_order": "ascending"},
    {"name": "duplicate_id", "type": "string", "sort_order": "ascending"},
    {"name": "table_range", "type": "string"},
    {"name": "meta", "type": "string"}
]

FILES_TABLE_SCHEMA = [
    {"name": "resource_id", "type": "string", "sort_order": "ascending"},
    {"name": "duplicate_id", "type": "string", "sort_order": "ascending"},
    {"name": "filename", "type": "string", "sort_order": "ascending"},
    {"name": "sha1", "type": "string"}
]

def main():
    parser = argparse.ArgumentParser(description="Create tables.")
    parser.add_argument("--proxy", help='Proxy alias')
    args = parser.parse_args()

    client = yt.wrapper.YtClient(proxy=args.proxy)

    common_attributes = {
        "dynamic": True,
        "tablet_cell_bundle": "yt-skynet-m5r",
    }

    client.create("table", REQUESTS_TABLE, attributes=dict(
        schema=REQUESTS_TABLE_SCHEMA,
        **common_attributes
    ))
    client.mount_table(REQUESTS_TABLE)

    client.create("table", RESOURCES_TABLE, attributes=dict(
        schema=RESOURCES_TABLE_SCHEMA,
        **common_attributes
    ))
    client.mount_table(RESOURCES_TABLE)


if __name__ == "__main__":
    main()
