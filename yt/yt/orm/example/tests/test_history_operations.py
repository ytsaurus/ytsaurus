from .conftest import Cli, ExampleTestEnvironment

from yt.orm.tests.helpers import UpdateHistoryIndexCli

from yt.orm.library.common import wait, YtResponseError

from yt.yt.orm.example.python.admin.db_operations import EXAMPLE_DATA_MODEL_TRAITS

from yt.yt.orm.example.python.client.client import EXAMPLE_CLIENT_TRAITS

from yt.wrapper.common import generate_uuid

import yt.yson as yson

import yt.wrapper as yt

import yatest.common

import copy
import datetime
import pytest


class ExampleUpdateHistoryIndexCli(UpdateHistoryIndexCli):
    DATA_MODEL_TRAITS = EXAMPLE_DATA_MODEL_TRAITS
    CLIENT_TRAITS = EXAMPLE_CLIENT_TRAITS


class TrimHistoryCli(Cli):
    def __init__(self):
        super(TrimHistoryCli, self).__init__(
            yatest.common.binary_path("yt/yt/orm/tools/history/trim_history/trim_history")
        )


class TestIndex:
    def test(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        yt_client = example_env.yt_client
        config = {
            "object_manager": {
                "history_index_mode_per_type_per_attribute": {
                    "book": {
                        "/spec/digital_data/available_formats": "disabled",
                    }
                }
            }
        }
        example_env.dynamic_config.update_config(config)

        publisher = example_env.create_publisher()
        book = str(example_env.create_book(publisher))
        example_env.client.update_object(
            "book", book, set_updates=[{"path": "/spec/page_count", "value": 42}]
        )
        example_env.client.update_object(
            "book",
            book,
            set_updates=[{"path": "/spec/digital_data/available_formats", "value": []}],
        )

        formats = [
            {
                "format": "pdf",
                "size": 12778383,
            }
        ]
        example_env.client.update_object(
            "book",
            book,
            set_updates=[{"path": "/spec/digital_data/available_formats", "value": formats}],
        )
        example_env.client.update_object(
            "book",
            book,
            set_updates=[{"path": "/spec/digital_data/available_formats", "value": formats}],
        )

        events = example_env.client.select_object_history(
            "book",
            book,
            ["/spec/digital_data/available_formats"],
            options=dict(distinct=True),
        )["events"]
        assert len(events) == 2
        with pytest.raises(YtResponseError):
            example_env.client.select_object_history(
                "book",
                book,
                selectors=["/spec/digital_data/available_formats"],
                options=dict(distinct=True, index_mode="enabled"),
            )

        config["object_manager"]["history_index_mode_per_type_per_attribute"]["book"][
            "/spec/digital_data/available_formats"
        ] = "building"
        example_env.dynamic_config.update_config(config)

        ExampleUpdateHistoryIndexCli.do_update_index(
            yt_client,
            "//home/example",
            *for_both_history_tables,
            "book",
            ["/spec/digital_data/available_formats"],
            "logical",
        )

        formats.append(
            {
                "format": "epub",
                "size": 127785201,
            }
        )
        example_env.client.update_object(
            "book",
            book,
            set_updates=[{"path": "/spec/digital_data/available_formats", "value": formats}],
        )
        example_env.client.update_object(
            "book", book, set_updates=[{"path": "/spec/page_count", "value": 420}]
        )
        example_env.client.update_object(
            "book", book, set_updates=[{"path": "/spec/digital_data/store_rating", "value": 7.9}]
        )

        example_env.client.update_object(
            "book", book, remove_updates=[{"path": "/spec/digital_data/available_formats"}]
        )
        example_env.client.remove_object("book", book)

        events_without_index = example_env.client.select_object_history(
            "book",
            book,
            ["/spec/digital_data/available_formats"],
            options=dict(distinct=True),
        )["events"]
        events_with_index = example_env.client.select_object_history(
            "book",
            book,
            ["/spec/digital_data/available_formats"],
            options=dict(distinct=True, index_mode="enabled"),
        )["events"]

        assert len(events_without_index) == len(events_with_index) == 5

    def test_no_excessive_updates(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        example_env.clean_history()

        yt_client = example_env.yt_client
        book = example_env.create_book()
        orm_path = "//home/example"

        example_env.client.update_object("book", book, set_updates=[{"path": "/spec/year", "value": 2001}])
        example_env.client.update_object("book", book, set_updates=[{"path": "/spec/font", "value": "Font 1"}])
        example_env.client.update_object("book", book, set_updates=[{"path": "/spec/year", "value": 2002}])

        ExampleUpdateHistoryIndexCli.do_update_index(
            yt_client,
            orm_path,
            *for_both_history_tables,
            "book",
            ["/spec/font"],
            "logical",
        )

        query = f"* from [{orm_path}/db/{for_both_history_tables[1]}]"
        rows = list(yt_client.select_rows(query))
        assert 2 == len(rows)


class TestHistoryTrimmer:
    def test(self, example_env, for_both_history_tables):
        client = example_env.client
        yt_client = example_env.yt_client
        db_manager = example_env.db_manager

        history_tables = for_both_history_tables
        history_events, history_index = history_tables
        for table in history_tables:
            table_path = f"//home/example/db/{table}"
            db_manager.unmount_tables([table_path])
            yt_client.set(yt.ypath_join(table_path, "@enable_dynamic_store_read"), True)
            db_manager.mount_tables([table_path])

        config_path = yatest.common.work_path("config.yson")
        cold_history_path = f"//home/example/cold_history_{generate_uuid()}"
        intermediate_results_path = f"//home/example/intermediate{generate_uuid()}"

        publisher = client.create_object("publisher")
        client.update_object(
            "publisher", publisher, [{"path": "/spec/name", "value": "Henry"}]
        )
        book = client.create_object(
            "book", attributes=dict(meta=dict(isbn="978-1449355739", parent_key=publisher))
        )
        client.update_object(
            "book",
            book,
            set_updates=[
                {
                    "path": "/spec/digital_data/available_formats",
                    "value": [{"format": "unknown", "size": 64}],
                    "recursive": True,
                }
            ],
        )
        result_rows = dict()

        def decrease_time(row, delta_time):
            if "time" in row:
                row["time"] -= delta_time
                return

            assert "inverted_time" in row
            row["inverted_time"] += delta_time

        def extract_time(row):
            if "time" in row:
                return row["time"]

            assert "inverted_time" in row
            return 2**64 - row["inverted_time"] - 1

        def events_handler(rows):
            assert 4 == len(rows)

            now = datetime.datetime.utcnow()
            # Book.
            decrease_time(rows[0], 2 * 24 * 3600 * 2**30)  # 2 days ago.

            # Publisher
            decrease_time(rows[2], 3 * 24 * 3600 * 2**30)  # 3 days ago.
            decrease_time(rows[3], 1 * 24 * 3600 * 2**30)  # 1 day ago.

            for row_index, day_diff in ((2, 3), (0, 2)):
                table_name = (now - datetime.timedelta(days=day_diff)).strftime("%Y-%m-%d")
                row = rows[row_index].copy()
                if "hash" in row:
                    del row["hash"]
                result_rows[yt.ypath_join(cold_history_path, history_events, table_name)] = [row]

            result_rows[f"//home/example/db/{history_events}"] = [rows[1].copy(), rows[3].copy()]

        def index_handler(rows):
            assert 2 == len(rows)
            decrease_time(rows[0], 2 * 24 * 3600 * 2**30)  # 2 days ago.

            result_rows[f"//home/example/db/{history_index}"] = [rows[1].copy()]

        for table, handler in (
            (history_events, events_handler),
            (history_index, index_handler),
        ):

            def get_non_key_fields(table):
                return list(
                    map(
                        lambda val: val["name"],
                        filter(
                            lambda val: val["name"] == "hash" or "sort_order" not in val,
                            yt_client.get(f"//home/example/db/{table}/@schema"),
                        ),
                    )
                )

            actual_rows = list(yt_client.select_rows(f"* from [//home/example/db/{table}] LIMIT 100"))
            to_delete = copy.deepcopy(actual_rows)
            for row in to_delete:
                list(map(row.pop, get_non_key_fields(table)))

            handler(actual_rows)

            for row in actual_rows:
                if "hash" in row:
                    del row["hash"]
            yt_client.delete_rows(f"//home/example/db/{table}", to_delete)
            yt_client.insert_rows(f"//home/example/db/{table}", actual_rows)

        proxy_url = yt_client.config["proxy"]["url"]
        yt_client.create("map_node", cold_history_path, recursive=True)
        yt_client.create("map_node", intermediate_results_path, recursive=True)

        with open(config_path, "wb") as f:
            f.write(
                yson.dumps(
                    {
                        # Using proxy url because access by cluster does not work in tests.
                        "source_cluster": proxy_url,
                        "orm_path": "//home/example",
                        "orm_instance_name": "test",
                        "intermediate_results_path": intermediate_results_path,
                        "destination_cluster": proxy_url,
                        "cold_history_path": cold_history_path,
                        "history_ttl_days": 1,
                        "history_time_mode": "logical",
                        "per_table_trim_policy": {
                            history_events: "move_to_cold_history",
                            history_index: "remove"
                        }
                    }
                )
            )

        cli = TrimHistoryCli()
        cli.check_output(["--config-path", config_path])

        assert 0 == len(yt_client.list(yt.ypath_join(intermediate_results_path, history_events)))
        days = yt_client.list(yt.ypath_join(cold_history_path, history_events), absolute=True)
        assert 2 == len(days)
        result_tables = {path for path in result_rows.keys()}
        assert set(days).union(set((f"//home/example/db/{history_events}", f"//home/example/db/{history_index}"))) == result_tables
        history_schema = yt_client.get(f"//home/example/db/{history_events}/@schema")
        cold_history_schema = yt_client.get(days[0] + "/@schema")

        def schema_fields(schema):
            return sorted([{"name": x["name"], "type": x["type"]} for x in schema if x["name"] != "hash"], key=lambda x : x["name"])

        assert schema_fields(history_schema) == schema_fields(cold_history_schema)

        outdated_time = None
        for table_path, expected_rows in result_rows.items():
            actual_rows = list(yt_client.read_table(table_path))
            assert actual_rows == expected_rows, "Inconsistency at path " + table_path
            if not table_path.startswith("//home/example/db"):
                outdated_time = extract_time(expected_rows[0])

        def throws(index_mode):
            try:
                client.select_object_history(
                    "book",
                    book,
                    ["/spec/digital_data/available_formats"],
                    options=dict(timestamp_interval=[outdated_time, None], index_mode=index_mode, distinct=True),
                )
            except YtResponseError:
                return True
            else:
                return False

        wait(lambda: throws(index_mode="enabled"))
        wait(lambda: throws(index_mode="disabled"))
