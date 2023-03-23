#!/usr/bin/python3

import argparse
import json
from pathlib import Path
import random
import sys
from time import time
from typing import Dict, List

from faker import Faker
from faker.providers import internet
from faker.providers import company

import yt.wrapper


class UpdateTutorialDataError(RuntimeError):
    pass


def get_staff_data_in_string(data) -> str:
    rows = [f"{{\"login\"=\"{data[i][0]}\";\"name\"=\"{data[i][1]}\";\"uid\"={data[i][2]}}};" for i in range(
        len(data))]
    return "".join(rows)


def generate_data(name_to_data_map: Dict[str, str], seed: int = 1111, number_of_rows: int = 10**4):
    fake = Faker()
    Faker.seed(seed)
    fake.add_provider(internet)
    fake.add_provider(company)
    random.seed(seed)

    uids = random.sample(range(10**16, 10**17), number_of_rows)
    staff_rows = [(fake.user_name(), fake.name(), uids[i])
                  for i in range(number_of_rows)]
    sorted_by_name_data = sorted(staff_rows, key=lambda tup: tup[1])
    staff_sample_rows = [
        f"{{\"name\"=\"{staff_rows[i][1]}\";\"uid\"={staff_rows[i][2]}}};" for i in range(10)]
    is_robot_rows = [
        f"{{\"is_robot\"=%{random.choice(['true', 'false'])};\"uid\"={row[2]}}};" for row in staff_rows]
    doc_title_rows = [
        f"{{\"host\"=\"{fake.url()}\";\"path\"=\"{fake.uri_path()}\";\"title\"=\"{fake.catch_phrase()}\"}};" for _ in range(number_of_rows)]

    name_to_data_map["staff_unsorted"] = get_staff_data_in_string(staff_rows)
    name_to_data_map["staff_sorted"] = get_staff_data_in_string(
        sorted_by_name_data)
    name_to_data_map["staff_unsorted_sample"] = "".join(staff_sample_rows)
    name_to_data_map["staff_unsorted_schematized"] = get_staff_data_in_string(
        staff_rows)
    name_to_data_map["staff_unsorted_schematized.schema"] = json.dumps(
        [
            {"type_v3": {"type_name": "optional", "item": "string"},
                "type": "string", "required": False, "name": "login"},
            {"type_v3": {"type_name": "optional", "item": "string"},
                "type": "string", "required": False, "name": "name"},
            {"type_v3": {"type_name": "optional", "item": "int64"},
                "type": "int64", "required": False, "name": "uid"}
        ]
    )
    name_to_data_map["is_robot_unsorted"] = "".join(is_robot_rows)
    name_to_data_map["doc_title"] = "".join(doc_title_rows)
    name_to_data_map["links_sorted_schematized"] = (
        "{\"DocTitle\"=\"doc_a\";\"Link\"={\"Host\"=\"ya.ru\";\"Port\"=80;\"Path\"=\"/mail\";};\"OccurenceCount\"=10u;};"
        "{\"DocTitle\"=\"doc_a\";\"Link\"={\"Host\"=\"google.com\";\"Port\"=80;\"Path\"=\"/gmail\";};\"OccurenceCount\"=15u;};"
    )
    name_to_data_map["links_sorted_schematized.schema"] = json.dumps(
        [
            {'name': 'DocTitle', 'required': False, 'sort_order': 'ascending',
                'type': 'string', 'type_v3': {'type_name': 'optional', 'item': 'string'}},
            {'name': 'Link', 'required': False, 'type': 'any', 'type_v3':
                {'type_name': 'optional', 'item':
                 {'type_name': 'struct', 'members':
                  [
                      {'type': {'type_name': 'optional',
                                'item': 'string'}, 'name': 'Host'},
                      {'type': {'type_name': 'optional',
                                'item': 'int32'}, 'name': 'Port'},
                      {'type': {'type_name': 'optional',
                                'item': 'string'}, 'name': 'Path'}
                  ]
                  }}},
            {'name': 'OccurenceCount', 'required': False, 'type': 'uint32',
                'type_v3': {'type_name': 'optional', 'item': 'uint32'}}
        ]
    )
    name_to_data_map["host_video_regexp"] = (
        "{\"host\"=\"http://www.nature.com\";\"video_regexp\"=\"\";};"
        "{\"host\"=\"https://arxiv.org\";\"video_regexp\"=\"\";};"
        "{\"host\"=\"https://en.wikipedia.org\";\"video_regexp\"=\"\";};"
        "{\"host\"=\"https://github.com\";\"video_regexp\"=\"\";};"
        "{\"host\"=\"https://ru.wikipedia.org\";\"video_regexp\"=\"\";};"
        "{\"host\"=\"https://vimeo.com\";\"video_regexp\"=\"^/[0-9]+$\";};"
        "{\"host\"=\"https://vk.com\";\"video_regexp\"=\"^/video.*$\";};"
        "{\"host\"=\"https://www.youtube.com\";\"video_regexp\"=\"^/watch.*$\";};"
    )


def save_files(path_to_directory: str, name_to_data_map: Dict[str, str]):
    Path(path_to_directory).mkdir(parents=True, exist_ok=True)
    for file_name in name_to_data_map.keys():
        with open(f"{path_to_directory.removesuffix('/')}/{file_name}", "w", encoding="utf-8") as file:
            print(f"{file.name} saved", file=sys.stderr)
            file.write(name_to_data_map[file_name])


def upload_files_to_map(file_names: List[str], path_to_directory: str, name_to_data_map: Dict[str, str]):
    for file_name in file_names:
        full_file_name = f"{path_to_directory.removesuffix('/')}/{file_name}"
        with open(full_file_name, "r", encoding="utf-8") as file:
            name_to_data_map[file_name] = file.read()
        try:
            with open(f"{full_file_name}.schema", "r", encoding="utf-8") as file:
                name_to_data_map[f"{file_name}.schema"] = file.read()
        except FileNotFoundError:
            pass


def create_tables(file_names: List[str], args: argparse.Namespace, name_to_data_map: Dict[str, str]):
    yt.wrapper.config.set_proxy(args.proxy)

    if not yt.wrapper.exists(args.yt_directory):
        raise UpdateTutorialDataError(
            f"No such directory: {args.yt_directory}")

    if yt.wrapper.get(f"{args.yt_directory.removesuffix('/')}/@count") != 0 and not args.force:
        raise UpdateTutorialDataError(
            f"Directory `{args.yt_directory}` isn't empty")

    for file_name in file_names:
        table_path = f"{args.yt_directory.removesuffix('/')}/{file_name}"

        data = name_to_data_map[file_name]
        schema = name_to_data_map.get(f"{file_name}.schema")

        if schema is not None:
            yt.wrapper.create("table", table_path, attributes={
                              "schema": json.loads(schema)}, ignore_existing=True)

        yt.wrapper.write_table(table_path, data, format="yson", raw=True)
        print(f"Table {file_name} was successfully created", file=sys.stderr)


def main():
    t1 = time()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-g", "--generate", help="Generate tables for examples", action="store_true")
    parser.add_argument(
        "-w", "--write", help="Write tables to YT", action="store_true")
    parser.add_argument("--save-to-directory",
                        help="Directory for generated tables")
    parser.add_argument("--files-directory",
                        help="Directory with tables for writing to YT")
    parser.add_argument("--proxy", help="YTsaurus cluster")
    parser.add_argument(
        "--yt-directory", help="Directory for creating tables", default="//home/tutorial")
    parser.add_argument(
        "-f", "--force", help="Ignore that YT directory isn't empty", action="store_true")

    name_to_data_map = {}

    args = parser.parse_args()

    if args.generate:
        print("Files generation started...", file=sys.stderr)
        generate_data(name_to_data_map, args.save_to_directory)
        print("Files was successfully generated!", file=sys.stderr)

        if args.save_to_directory:
            save_files(args.save_to_directory, name_to_data_map)
            print("Files was successfully saved!", file=sys.stderr)

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

    if args.write:
        if args.files_directory:
            upload_files_to_map(
                file_names, args.files_directory, name_to_data_map)
            print("Files was successfully uploaded!", file=sys.stderr)
        create_tables(file_names, args, name_to_data_map)

    print("All done!", file=sys.stderr)
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
