import datetime
import random
import typing
import time
import uuid
import yt.wrapper
import sys
import os
import argparse
import json
import tarfile
import tempfile
from string import Template
from yt import yson
from typing import Dict, List
from yt.wrapper.schema import OutputRow
from datetime import timedelta, date
from zstandard import ZstdDecompressor


class TutorialGenerateError(RuntimeError):
    pass


@yt.wrapper.yt_dataclass
class Nomenclature:
    id: int
    name: str
    is_rx: bool
    first_appeared: int
    meta_data: yt.wrapper.schema.YsonBytes


@yt.wrapper.yt_dataclass
class Prices:
    nomenclature_id: int
    date: date
    price: float
    min_price: float


@yt.wrapper.yt_dataclass
class Orders:
    date: date
    nomenclature_id: int
    order_uuid: str
    quantity: int
    order_meta: yt.wrapper.schema.YsonBytes


@yt.wrapper.yt_dataclass
class PricesSplit:
    price_date: int


@yt.wrapper.yt_dataclass
class TutorialQueryId:
    query_id: str


FILE_NAMES = [
    "staff_unsorted",
    "staff_sorted",
    "staff_unsorted_sample",
    "staff_unsorted_schematized",
    "is_robot_unsorted",
    "doc_title",
    "links_sorted_schematized",
    "host_video_regexp",
]

DATA_DIRECTORY_PATH = "data"

ARCHIVE_NAME = "data.tar.zst"

SCHEMA_NOMENCLATURE = yson.to_yson_type(
    [
        {"name": "id", "type": "int64", "sort_order": "ascending"},
        {"name": "name", "type": "string"},
        {"name": "is_rx", "type": "boolean"},
        {"name": "first_appeared", "type": "timestamp"},
        {"name": "meta_data", "type": "any"},
    ],
    attributes={"unique_keys": True, "strict": True},
)


SCHEMA_PRICES = [
    {"name": "nomenclature_id", "type": "int64"},
    {"name": "date", "type": "date"},
    {"name": "price", "type": "double"},
    {"name": "min_price", "type": "double"},
]


SCHEMA_ORDERS = [
    {"name": "date", "type": "date"},
    {"name": "nomenclature_id", "type": "int64"},
    {"name": "order_uuid", "type": "string"},
    {"name": "quantity", "type": "int64"},
    {"name": "order_meta", "type": "any"},
]

SCHEMA_TUTORIAL_QUERY_ID = [{"name": "query_id", "type": "string", "required": True}]


def upload_files_to_map(
    file_names: List[str], data_directory_path: str, archive_name: str, name_to_data_map: Dict[str, str]
):
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
    yt_client = yt.wrapper.YtClient(proxy=args.proxy, token=os.environ["YT_TOKEN"])

    if not yt_client.exists(args.yt_directory):
        raise TutorialGenerateError(f"No such directory: {args.yt_directory}")

    if yt_client.get(f"{args.yt_directory.removesuffix('/')}/@count") != 0 and not args.force:
        raise TutorialGenerateError(f"Directory `{args.yt_directory}` isn't empty")

    for file_name in file_names:
        table_path = f"{args.yt_directory.removesuffix('/')}/{file_name}"

        data = name_to_data_map[file_name]
        schema = name_to_data_map.get(f"{file_name}.schema")

        if schema is not None:
            yt_client.create("table", table_path, attributes={"schema": json.loads(schema)}, ignore_existing=True)

        yt_client.write_table(table_path, data, format="yson", raw=True)
        print(f"Table {file_name} was successfully created", file=sys.stderr)


def generate_name(prefixes: [], vowels: [], consonants: []) -> str:
    return (
        str.upper(random.choice(consonants))
        + random.choice(vowels)
        + random.choice(consonants)
        + random.choice(vowels)
        + random.choice(consonants)
        + random.choice(prefixes)
    )


def random_timestamp(start_date: date, end_date: date) -> int:
    start_ts = time.mktime(start_date.timetuple())
    end_ts = time.mktime(end_date.timetuple())
    random_ts = random.uniform(start_ts, end_ts)
    return int(round(random_ts * 1e6, 0))


def generate_nomenclature(size: int):
    prefixes = [
        'vir',
        'cillin',
        'mab',
        'ximab',
        'zumab',
        'anib',
        'ciclib',
        'degib',
        'denib',
        'lisib',
        'parib',
        'rafenib',
        'tinib',
        'zomib',
        'vastatin',
        'prazole',
        'lukast',
        'axine',
        'olol',
        'oxetine',
        'sartan',
        'pril',
        'oxacin',
        'xaban',
        'afil',
        'ine',
        'tide',
        'vec',
        'ast',
        'caine',
        'dipine',
        'tidine',
        'setron',
        'mycin',
    ]
    vowels = ['a', 'e', 'i', 'o', 'u', 'y']
    consonants = [
        'b',
        'c',
        'd',
        'f',
        'g',
        'h',
        'j',
        'k',
        'l',
        'm',
        'n',
        'p',
        'q',
        'r',
        's',
        't',
        'v',
        'w',
        'x',
        'y',
        'z',
    ]
    nomenclature = []
    existed_names = {}
    for i in range(min(size, 10000000)):
        name = generate_name(prefixes=prefixes, vowels=vowels, consonants=consonants)
        while name in existed_names.keys():
            name = generate_name(prefixes=prefixes, vowels=vowels, consonants=consonants)
        existed_names.update({name: 1})
        nomenclature.append(
            Nomenclature(
                id=i,
                name=name,
                is_rx=random.choice([True, False]),
                first_appeared=random_timestamp(datetime.date(2007, 1, 1), datetime.date.today()),
                meta_data=yt.wrapper.schema.YsonBytes(
                    yson.dumps(
                        {
                            "min_temperature": random.choice([-5, -10, -20]),
                            "max_temperature": random.choice([5, 10, 20, 25]),
                            "origin": random.choice(
                                [
                                    "Russia",
                                    "China",
                                    "United States",
                                    "Japan",
                                    "Germany",
                                    "France",
                                    "United Kingdom",
                                    "India",
                                    "Italy",
                                ]
                            ),
                        }
                    )
                ),
            )
        )
    return nomenclature


class PriceMapper(yt.wrapper.TypedJob):
    def __init__(self, size):
        self._size = size

    def __call__(self, input_row: PricesSplit) -> typing.Iterable[Prices]:
        nomenclature_id = 0
        for nomenclature_id in range(self._size):
            price = round(random.uniform(1, 100), 2)
            dt = datetime.date.today() - timedelta(days=input_row.price_date)
            min_price = round(price * random.uniform(0.5, 0.9), 2)
            nomenclature_id = nomenclature_id + 1
            yield Prices(nomenclature_id=nomenclature_id, price=price, date=dt, min_price=min_price)


class GenerateOrders(yt.wrapper.TypedJob):
    def __init__(self, nomenclature_count, max_order_size, desired_orders_size):
        self.nomenclature_count = nomenclature_count
        self.max_order_size = max_order_size
        self.desired_orders_size = desired_orders_size

    def __call__(self, input_row: PricesSplit) -> typing.Iterable[OutputRow[Orders]]:
        current_orders_size = 0
        while current_orders_size < self.desired_orders_size:
            order_uuid = str(uuid.uuid4())
            order_size = random.randrange(self.max_order_size)
            unique_nomenclatures = random.sample(range(self.nomenclature_count), k=order_size)
            current_orders_size += order_size
            for nomenclature_id in unique_nomenclatures:
                yield OutputRow(
                    Orders(
                        order_uuid=order_uuid,
                        date=datetime.date.today() - timedelta(days=input_row.price_date),
                        nomenclature_id=nomenclature_id,
                        quantity=random.randrange(1, 100),
                        order_meta=yt.wrapper.schema.YsonBytes(
                            yson.dumps(
                                {
                                    "warehouse_rack": random.randrange(1, 10000),
                                    "order_collector": random.randrange(1, 2500),
                                    "quality": random.choices(["Good", "Damaged"], weights=[1000, 1], k=1),
                                }
                            )
                        ),
                    ),
                    table_index=input_row.price_date,
                )


def generate_data(args: argparse.Namespace):
    client = yt.wrapper.YtClient(proxy=args.proxy, token=os.environ["YT_TOKEN"])

    path_to_nomenclature_table = args.yt_directory + "/nomenclature"
    path_to_prices_table = args.yt_directory + "/price"
    path_to_price_map_table = args.yt_directory + "/price_map"
    path_to_orders_directory = args.yt_directory + "/orders"

    if not client.exists(args.yt_directory) and not args.create_directory:
        raise TutorialGenerateError(f"No such directory: {args.yt_directory}")

    if client.exists(args.yt_directory) and not args.force:
        raise TutorialGenerateError("Directory already exists and force flag is false")
    else:
        client.create("map_node", path_to_orders_directory, recursive=True, force=args.force)

    if not args.force:
        for table in [
            path_to_nomenclature_table,
            path_to_prices_table,
            path_to_price_map_table,
            path_to_orders_directory,
        ]:
            if client.exists(table):
                raise TutorialGenerateError(f"Table {table} exists and force flag is false")

    with client.Transaction():
        client.create(
            type="table",
            force=args.force,
            path=path_to_nomenclature_table,
            attributes={
                "schema": SCHEMA_NOMENCLATURE,
                "optimize_for": "lookup",
                "chunk_writer": {"block_size": 256 * 2**10},
                "desired_chunk_size": 100 * 2**20,
            },
        )

        client.create(
            type="table",
            force=args.force,
            path=path_to_prices_table,
            attributes={"schema": SCHEMA_PRICES, "optimize_for": "scan"},
        )

        client.write_table_structured(
            table=path_to_nomenclature_table,
            row_type=Nomenclature,
            input_stream=generate_nomenclature(args.nomenclature_count),
        )
        client.run_sort(source_table=path_to_nomenclature_table, sort_by=["id"])

        client.write_table_structured(
            table=path_to_price_map_table,
            row_type=PricesSplit,
            input_stream=[PricesSplit(i) for i in range(args.days_to_generate)],
        )

        output_order_tables = []
        client.create("map_node", path_to_orders_directory, force=args.force)

        for day_index in range(args.days_to_generate):
            order_table_path = path_to_orders_directory + "/" + str(datetime.date.today() - timedelta(days=day_index))
            output_order_tables.append(order_table_path)
            client.create(
                type="table",
                force=args.force,
                path=order_table_path,
                recursive=True,
                attributes={"schema": SCHEMA_ORDERS, "optimize_for": "scan"},
            )

        client.run_map(
            GenerateOrders(
                nomenclature_count=args.nomenclature_count,
                max_order_size=args.max_order_size,
                desired_orders_size=args.desired_order_size,
            ),
            source_table=path_to_price_map_table,
            destination_table=output_order_tables,
            job_count=args.max_job_count,
        )

        client.run_map(
            PriceMapper(size=args.nomenclature_count),
            source_table=path_to_price_map_table,
            destination_table=path_to_prices_table,
            job_count=args.max_job_count,
        )

        client.run_sort(source_table=path_to_prices_table, sort_by=["date", "nomenclature_id"])

    client.remove(path=path_to_price_map_table)
    client.alter_table(path=path_to_nomenclature_table, dynamic=True)
    client.set(f"{path_to_nomenclature_table}/@enable_dynamic_store_read", True)
    client.mount_table(path=path_to_nomenclature_table, sync=True)


def upload_tutorials(args: argparse.Namespace):
    client = yt.wrapper.YtClient(proxy=args.proxy, token=os.environ["YT_TOKEN"])
    path_to_query_ids_table = args.yt_directory + "/query_ids"
    if args.full_wipe_annotations:
        full_queries = client.list_queries(stage=args.stage, filter="is_tutorial")["queries"]
        print(f"Full queries: {full_queries}")
        for query in full_queries:
            if "is_tutorial" in query["annotations"]:
                print(f"Removing annotation from query {query['id']}")
                patched_annotation = query["annotations"].copy()
                patched_annotation.pop("is_tutorial", None)
                client.alter_query(query_id=query["id"], stage=args.stage, annotations={**patched_annotation})

    if client.exists(path_to_query_ids_table):
        query_ids = client.read_table_structured(table=path_to_query_ids_table, row_type=TutorialQueryId)
        for query_id in query_ids:
            try:
                client.alter_query(query_id=query_id.query_id, stage=args.stage, annotations={})
            except Exception as e:
                print(f"Failed to remove annotation from query {query_id.query_id}")
                print(str(e).rstrip("\n"))

    client.create(
        type="table",
        force=args.force,
        path=path_to_query_ids_table,
        attributes={"schema": SCHEMA_TUTORIAL_QUERY_ID, "optimize_for": "scan"},
    )

    path_to_tables_directory = args.yt_directory
    root_dir = args.scripts_folder
    query_ids = []
    for root, dirs, files in os.walk(root_dir):
        files.sort(reverse=True)
        if files:
            print(f" Root: {root}, Dirs: {dirs}, Files: {files}")
            for file in files:
                print(f"Engine: {root.split("/")[1]}")
                file_path = os.path.join(root, file)
                with open(file_path, "r") as f:
                    script = Template(f.read()).safe_substitute(
                        nomenclature=path_to_tables_directory + "/nomenclature",
                        price=path_to_tables_directory + "/price",
                        orders=path_to_tables_directory + "/orders",
                        start_date=datetime.date.today() - timedelta(days=args.days_to_generate - 1),
                        end_date=datetime.date.today(),
                    )
                    query_id = yt.wrapper.driver.make_request(
                        "start_query",
                        {
                            "engine": root.split("/")[1],
                            "query": script,
                            "settings": {"cluster": args.cluster_name} if args.cluster_name else {},
                            "access_control_objects": ["everyone"],
                            "annotations": {"title": (root.split("/")[1]).upper() + " " + file, "is_tutorial": True},
                            "draft": True,
                            "stage": args.stage,
                        },
                        client=client,
                    )
                    query_ids.append(TutorialQueryId(query_id=json.loads(query_id.decode("utf-8"))["query_id"]))
                    print(f"Processed script: {file_path}\n{script}\n")

    client.write_table_structured(table=path_to_query_ids_table, row_type=TutorialQueryId, input_stream=query_ids)


def main():
    t1 = time.time()
    parser = argparse.ArgumentParser()

    parser.add_argument("--proxy", help="Path to YTsaurus cluster", required=True)
    parser.add_argument("--yt-directory", help="Directory for creating tables", required=True)
    parser.add_argument("--create-directory", help="Create directory if not exists", default=True)
    parser.add_argument("--max-job-count", help="Max job count in operation", default=100, type=int)
    parser.add_argument(
        "--nomenclature-count", help="Number of nomenclatures, defines size of dynamic table", default=10000, type=int
    )
    parser.add_argument("--days-to-generate", help="Number of days in price and orders tables", default=7, type=int)
    parser.add_argument("--max-order-size", help="Max order size in orders table", default=200, type=int)
    parser.add_argument(
        "--desired-order-size", help="Desired row number per day in orders table", default=2000, type=int
    )
    parser.add_argument("-f", "--force", help="Ignore that YT directory isn't empty", action="store_true")
    parser.add_argument("--scripts-folder", help="Folder with queries templates", default="scripts")
    parser.add_argument(
        "--full-wipe-annotations", help="Full wipe non-empty query annotations for user", action="store_true"
    )
    parser.add_argument("--cluster-name", help="Name of the target cluster")
    parser.add_argument("--stage", help="Stage for queries", default="experimental")

    args = parser.parse_args()

    name_to_data_map = {}

    upload_files_to_map(FILE_NAMES, DATA_DIRECTORY_PATH, ARCHIVE_NAME, name_to_data_map)

    create_tables(FILE_NAMES, args, name_to_data_map)

    upload_tutorials(args=args)

    generate_data(args=args)

    print("Files was successfully uploaded!", file=sys.stderr)
    t2 = time.time()
    print(f"Run time: {round((t2 - t1) // 60)} min, {round((t2 - t1) % 60, 1)} sec...")


if __name__ == '__main__':

    try:
        main()
    except TutorialGenerateError as e:
        print("", file=sys.stderr)
        print("Error occurred...", file=sys.stderr)
        print("", file=sys.stderr)
        print(str(e).rstrip("\n"), file=sys.stderr)
        exit(1)
