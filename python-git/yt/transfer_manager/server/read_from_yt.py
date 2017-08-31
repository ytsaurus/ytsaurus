#!/usr/bin/env python

import sys
import os
import argparse
import tarfile
import json
import struct
from copy import deepcopy

READ_CHUNK_SIZE = 128 * 1024 * 1024

def merge_limits(table_path, limits):
    ranges = deepcopy(table_path.attributes.get("ranges", []))

    assert len(ranges) <= 1, "Table with multiple ranges cannot be copied"

    if ranges:
        for limit_name in limits:
            if limit_name not in ranges[0]:
                ranges[0][limit_name] = limits[limit_name]
                continue
            for type in limits[limit_name]:
                if type in ranges[0][limit_name]:
                    func = None
                    if limit_name == "lower_limit":
                        func = max
                    if limit_name == "upper_limit":
                        func = min
                    ranges[0][limit_name][type] = func(ranges[0][limit_name][type], limits[limit_name][type])
                else:
                    ranges[0][limit_name][type] = limits[limit_name][type]
    else:
        ranges = [limits]

    path = deepcopy(table_path)
    path.attributes["ranges"] = ranges

    return path

def load(input_type):
    if input_type == "json":
        for line in sys.stdin:
            yield json.loads(line)
    elif input_type == "lenval":
        count = 0
        while True:
            s = sys.stdin.read(4)
            if not s:
                break
            length = struct.unpack('i', s)[0]
            data = sys.stdin.read(length)
            if count % 3 == 2:
                yield json.loads(data)
            count += 1
    else:
        assert False, "Incorrect input format: " + input_type

def read(proxy, token, config, transaction, format, table, input_type):
    # We should import yt here since it can be unpacked in from local archive.
    from yt.wrapper import YtClient
    from yt.wrapper.common import chunk_iter_stream
    import yt.wrapper as yt

    client = YtClient(proxy, token=token, config=config)
    with client.Transaction(ping=False, transaction_id=transaction):
        table_path = yt.TablePath(table, client=client)
        for limits in load(input_type):
            merged_table_path = merge_limits(table_path, limits)
            read_stream = client.read_table(merged_table_path, format=format, raw=True)
            # Restore row checks?
            for chunk in chunk_iter_stream(read_stream, READ_CHUNK_SIZE):
                sys.stdout.write(chunk)

def main():
    parser = argparse.ArgumentParser(description="Command to read from yt cluster")
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--format", required=True)
    parser.add_argument("--input-type", required=True)
    parser.add_argument("--tx")
    parser.add_argument("--config-file", help="File with client config in JSON format")
    parser.add_argument("--package-file", action="append")
    args = parser.parse_args()

    if args.package_file:
        for package_file in args.package_file:
            assert package_file.endswith(".tar"), "Package must have .tar extension"
            tar = tarfile.open(package_file, "r:")
            tar.extractall()
            tar.close()
        sys.path.insert(0, ".")

    read(proxy=args.proxy,
         token=os.environ.get("YT_SECURE_VAULT_TOKEN"),
         config=json.load(open(args.config_file)),
         table=args.table,
         transaction=args.tx,
         format=args.format,
         input_type=args.input_type)

if __name__ == "__main__":
    main()
