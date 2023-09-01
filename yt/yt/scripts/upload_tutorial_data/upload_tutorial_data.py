#!/usr/bin/python3

import argparse
import json
import sys
from time import time
from typing import Dict, List
import tarfile
from zstandard import ZstdDecompressor
import tempfile

from yt.wrapper import YtClient


class UpdateTutorialDataError(RuntimeError):
    pass


def upload_files_to_map(file_names: List[str], data_directory_path: str, archive_name: str, name_to_data_map: Dict[str, str]):
    with tempfile.TemporaryFile(suffix=".tar") as tmp:
        with open(f"{data_directory_path}/{archive_name}", "rb") as f:
            ZstdDecompressor().copy_stream(f, tmp)
        tmp.seek(0)
        with tarfile.open("r", fileobj=tmp) as tar:
            for file_name in file_names:
                full_file_name = f"{archive_name.removesuffix('.tar.zst')}/{file_name}"
                with tar.extractfile(full_file_name) as file:
                    name_to_data_map[file_name] = file.read()
                try:
                    with tar.extractfile(f"{full_file_name}.schema") as file:
                        name_to_data_map[f"{file_name}.schema"] = file.read()
                except KeyError:
                    pass


def create_tables(file_names: List[str], args: argparse.Namespace, name_to_data_map: Dict[str, str]):
    yt_client = YtClient(proxy=args.proxy)

    if not yt_client.exists(args.yt_directory):
        raise UpdateTutorialDataError(
            f"No such directory: {args.yt_directory}")

    if yt_client.get(f"{args.yt_directory.removesuffix('/')}/@count") != 0 and not args.force:
        raise UpdateTutorialDataError(
            f"Directory `{args.yt_directory}` isn't empty")

    for file_name in file_names:
        table_path = f"{args.yt_directory.removesuffix('/')}/{file_name}"

        data = name_to_data_map[file_name]
        schema = name_to_data_map.get(f"{file_name}.schema")

        if schema is not None:
            yt_client.create("table", table_path, attributes={"schema": json.loads(schema)}, ignore_existing=True)

        yt_client.write_table(table_path, data, format="yson", raw=True)
        print(f"Table {file_name} was successfully created", file=sys.stderr)


def main():
    t1 = time()

    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", help="YTsaurus cluster")
    parser.add_argument(
        "--yt-directory", help="Directory for creating tables", default="//home/tutorial")
    parser.add_argument(
        "-f", "--force", help="Ignore that YT directory isn't empty", action="store_true")

    args = parser.parse_args()

    file_names = [
        "staff_unsorted",
        "staff_sorted",
        "staff_unsorted_sample",
        "staff_unsorted_schematized",
        "is_robot_unsorted",
        "doc_title",
        "links_sorted_schematized",
        "host_video_regexp"
    ]

    data_directory_path = "data"
    archive_name = "data.tar.zst"

    name_to_data_map = {}

    upload_files_to_map(
        file_names, data_directory_path, archive_name, name_to_data_map)
    create_tables(file_names, args, name_to_data_map)

    print("Files was successfully uploaded!", file=sys.stderr)
    t2 = time()
    print(
        f"Run time: {round((t2 - t1) // 60)} min, {round((t2 - t1) % 60, 1)} sec...")


if __name__ == "__main__":
    try:
        main()
    except UpdateTutorialDataError as e:
        print("", file=sys.stderr)
        print("Error occurred...", file=sys.stderr)
        print("", file=sys.stderr)
        print(str(e).rstrip("\n"), file=sys.stderr)
        exit(1)
