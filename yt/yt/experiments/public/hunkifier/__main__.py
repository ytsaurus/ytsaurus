import yt.wrapper as yt

import argparse


def hunkify(args, client):
    print("Table {} will be hunkified".format(args.table_path))

    schema = client.get("{}/@schema".format(args.table_path))

    for col in schema:
        if "sort_order" in col:
            assert col["sort_order"] == "ascending"
            continue
        if col["type"] != "string" and col["type"] != "any":
            continue
        if args.columns and col["name"] not in args.columns:
            continue
        col["max_inline_hunk_size"] = args.max_inline_hunk_size
        print("Column {} will be updated".format(col["name"]))

    print("Schema is updated")
    for col in schema:
        print(col)

    client.unmount_table(args.table_path, sync=True)

    print("Altering table schema")
    client.alter_table(args.table_path, schema=schema)

    if args.primary_medium is not None:
        print("Setting primary medium to {}".format(args.primary_medium))
        client.set("{}/@primary_medium".format(args.table_path), args.primary_medium)
    if args.enable_crp:
        print("Enabling crp")
        client.set("{}/@enable_consistent_chunk_replica_placement".format(args.table_path), True)
    if args.max_hunk_compaction_garbage_ratio is not None:
        print("Setting max hunk compaction garbage ratio to {}".format(args.max_hunk_compaction_garbage_ratio))
        client.set("{}/@max_hunk_compaction_garbage_ratio".format(args.table_path), args.max_hunk_compaction_garbage_ratio)
    if args.hunk_erasure_codec is not None:
        print("Setting hunk erasure codec to {}".format(args.hunk_erasure_codec))
        client.set("{}/@hunk_erasure_codec".format(args.table_path), args.hunk_erasure_codec)
    if args.fragment_read_hedging_delay is not None:
        print("Setting fragment read heding delay to {}".format(args.fragment_read_hedging_delay))
        if client.exists("{}/@hunk_chunk_reader".format(args.table_path)):
            client.set("{}/@hunk_chunk_reader/fragment_read_hedging_delay".format(args.table_path), args.fragment_read_hedging_delay)
        else:
            client.set("{}/@hunk_chunk_reader".format(args.table_path), {"fragment_read_hedging_delay": args.fragment_read_hedging_delay})
    if args.enable_slim_format:
        print("Enabling slim chunk format")
        client.set("{}/@optimize_for".format(args.table_path), "lookup")
        client.set("{}/@chunk_format".format(args.table_path), "table_versioned_slim")

    client.mount_table(args.table_path, sync=True)

    print("Table was successfully hunkified")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Hunkifier")

    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy", required=True)

    parser.add_argument("--table-path", type=str, help="Table path", required=True)
    parser.add_argument("--max-inline-hunk-size", type=int, help="Max inline hunk size", required=True)

    parser.add_argument("--primary-medium", type=str, help="Primary medium")
    parser.add_argument("--enable-crp", help="Enable consistent replica placement", action="store_true", dest="enable_crp")
    parser.add_argument("--max-hunk-compaction-garbage-ratio", type=float, help="Max hunk compaction garbage ratio")
    parser.add_argument("--hunk-erasure-codec", type=str, help="Hunk erasure codec")
    parser.add_argument("--fragment-read-hedging-delay", type=int, help="Fragment read hedging delay")
    parser.add_argument("--enable-slim-format", help="Enable slim chunk format", action="store_true", dest="enable_slim_format")
    parser.add_argument("--columns", help="Columns to hunkify", action="append")

    args = parser.parse_args()

    client = yt.YtClient(config=yt.config.config)

    hunkify(args, client)
