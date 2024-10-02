from yt.yt.orm.example.client.proto.data_model.autogen import schema_pb2
from yt.orm.library.common import wait, WatchesConfigurationMismatch, RowsAlreadyTrimmedError
from yt.orm.library.orchid_client import get_leader_fqdn

from yt.test_helpers.profiler import Profiler
from yt.wrapper.errors import YtTabletTransactionLockConflict, YtResponseError

from .conftest import format_object_key

from contextlib import nullcontext as does_not_raise

from datetime import timedelta
import pytest


class TestWatchLog:
    def test_unlimited_read(self, example_env):
        config = dict(
            watch_manager=dict(
                result_event_count_limit=50,
            )
        )
        with example_env.set_cypress_config_patch_in_context(config):
            start_timestamp = example_env.client.generate_timestamp()
            for i in range(51):
                example_env.client.create_object("executor")
            timestamp = example_env.client.generate_timestamp()

            result = example_env.client.watch_objects(
                "executor",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                time_limit=timedelta(seconds=30),
            )
            assert len(result) == 50

    def test_history_timestamp(self, example_env):
        start_timestamp = example_env.client.generate_timestamp()

        tx = example_env.client.start_transaction(start_timestamp=start_timestamp)
        example_env.client.create_object("executor", transaction_id=tx)
        example_env.client.commit_transaction(tx)

        timestamp = example_env.client.generate_timestamp()

        result = example_env.client.watch_objects(
            "executor",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=30),
        )
        assert len(result) == 1
        assert result[0]["history_timestamp"] == start_timestamp

    def test_filter(self, example_env):
        config = dict(
            watch_manager=dict(
                allow_filtering_by_meta=True,
                query_filter_enabled=True,
                labels_store_enabled=True,
            )
        )
        with example_env.set_cypress_config_patch_in_context(config):
            start_timestamp = example_env.client.generate_timestamp()
            executor_id = example_env.client.create_object("executor")
            example_env.client.create_object("executor")
            timestamp = example_env.client.generate_timestamp()

            result = example_env.client.watch_objects(
                "executor",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                time_limit=timedelta(seconds=30),
                filter=f"[/meta/id] = {executor_id}",
            )
            assert len(result) == 1

    def test_transaction_context(self, example_env):
        start_timestamp = example_env.client.generate_timestamp()

        transaction_context = {"key1": "will be overwritten", "key3": "value3"}
        tx = example_env.client.start_transaction(transaction_context=transaction_context)
        executor_id = example_env.client.create_object("executor", transaction_id=tx)

        example_env.client.update_object(
            "executor",
            executor_id,
            set_updates=[{"path": "/labels/tribe", "value": "Khalai"}],
            transaction_id=tx,
        )

        example_env.client.update_object(
            "executor",
            executor_id,
            set_updates=[{"path": "/spec/rank", "value": 3}],
            transaction_id=tx,
        )

        operation_data = {"key1": "value1", "key2": "value2"}
        example_env.client.commit_transaction(tx, operation_data)

        timestamp = example_env.client.generate_timestamp()

        result = example_env.client.watch_objects(
            "executor",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=30),
        )

        assert len(result) > 0
        merged_transaction_context = {"key1": "value1", "key2": "value2", "key3": "value3"}
        for event in result:
            assert event["transaction_context"]["items"] == merged_transaction_context

    @pytest.mark.parametrize(
        "skip,superuser,has_permission,skipped",
        [
            (True, True, False, True),
            (True, False, True, True),
            (True, False, False, False),
            (False, True, True, False),
        ],
    )
    def test_skip_watch_log_option(
        self,
        skip,
        superuser,
        has_permission,
        skipped,
        example_env,
        regular_user1,
    ):
        client = example_env.client
        if not superuser:
            client = regular_user1.client
            if has_permission:
                example_env.grant_permission(
                    "schema",
                    "executor",
                    "use",
                    regular_user1.id,
                    "/access/event_generation_skip_allowed",
                )
            example_env.grant_permission("schema", "executor", "create", regular_user1.id, "")

        start_timestamp = client.generate_timestamp()

        create_transaction = client.start_transaction(skip_watch_log=skip)
        executor_id = client.create_object("executor", transaction_id=create_transaction)
        client.commit_transaction(create_transaction)

        update_transaction = client.start_transaction(skip_watch_log=skip)
        client.update_object(
            "executor",
            executor_id,
            set_updates=[{"path": "/spec/rank", "value": 3}],
            transaction_id=update_transaction,
        )
        client.commit_transaction(update_transaction)

        remove_transaction = client.start_transaction(skip_watch_log=skip)
        client.remove_object("executor", executor_id, transaction_id=remove_transaction)
        client.commit_transaction(remove_transaction)

        end_timestamp = client.generate_timestamp()
        result = client.watch_objects(
            "executor",
            start_timestamp=start_timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=30),
        )

        if skipped:
            assert len(result) == 0
        else:
            assert len(result) != 0

    def test_meta(self, example_env):
        publisher = example_env.create_publisher()

        start_timestamp = example_env.client.generate_timestamp()
        book = example_env.create_book(publisher)
        example_env.client.update_object(
            "book", str(book), set_updates=[{"path": "/spec/font", "value": "Garamond"}]
        )
        example_env.client.remove_object("book", str(book))
        example_env.client.create_object(
            "employer",
            attributes=dict(
                meta=dict(id="John", passport="1234 567890", email="employer@yandex.ru"),
            ),
        )
        timestamp = example_env.client.generate_timestamp()

        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            request_meta_response=True,
        )

        for i in range(3):
            assert "isbn" in result[i]["meta"]
            assert "custom_meta_etc_test" not in result[i]["meta"]
        result = example_env.client.watch_objects(
            "employer",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            request_meta_response=True,
        )
        assert "passport" in result[0]["meta"]
        assert "email" not in result[0]["meta"]

    def test_read_by_tablets(self, example_env):
        executor_id = example_env.client.create_object("executor", request_meta_response=True)
        timestamp = example_env.client.generate_timestamp()

        tablet_count = example_env.yt_client.get("//home/example/db/executors_watch_log/@tablet_count")
        assert tablet_count > 0

        events = []
        continuation_token = None
        tablet_index = None
        for tablet in range(tablet_count):
            response = example_env.client.watch_objects(
                "executor",
                start_from_earliest_offset=True,
                tablets=[tablet],
                timestamp=timestamp,
                time_limit=timedelta(seconds=30),
                enable_structured_response=True,
            )
            if response.get("events"):
                for event in response["events"]:
                    if executor_id == event["object_id"]:
                        events.append(event)
                        assert continuation_token is None
                        continuation_token = response["continuation_token"]
                        tablet_index = tablet
                        break
            if events:
                break

        assert 1 == len(events)
        assert executor_id == events[0]["object_id"]

        example_env.client.update_object(
            "executor",
            executor_id,
            set_updates=[{"path": "/labels/tribe", "value": "Khalai"}],
        )

        timestamp = example_env.client.generate_timestamp()

        events = example_env.client.watch_objects(
            "executor",
            tablets=[tablet_index],
            timestamp=timestamp,
            time_limit=timedelta(seconds=30),
            continuation_token=continuation_token,
        )

        assert 1 == len(events)
        assert executor_id == events[0]["object_id"]

    def test_read_from_out_of_range_tablet(self, example_env):
        tablet_count = example_env.yt_client.get("//home/example/db/executors_watch_log/@tablet_count")
        with pytest.raises(YtResponseError):
            example_env.client.watch_objects(
                "executor",
                start_from_earliest_offset=True,
                tablets=[tablet_count],
            )

    def test_touch_object(self, example_env):
        for lock_removal in (False, True):
            executor_id = example_env.client.create_object("executor", request_meta_response=True)
            start_timestamp = example_env.client.generate_timestamp()

            tx1_id = example_env.client.start_transaction()
            tx2_id = example_env.client.start_transaction()

            example_env.client.update_object(
                "executor",
                executor_id,
                set_updates=[{"path": "/control/touch", "value": {"lock_removal": lock_removal}}],
                transaction_id=tx1_id,
            )

            example_env.client.remove_object("executor", executor_id, transaction_id=tx2_id)

            example_env.client.commit_transaction(tx1_id)
            if lock_removal:
                with pytest.raises(YtTabletTransactionLockConflict):
                    example_env.client.commit_transaction(tx2_id)
            else:
                example_env.client.commit_transaction(tx2_id)

        tx_id = example_env.client.start_transaction()
        executor_id = example_env.client.create_object(
            "executor",
            request_meta_response=True,
            transaction_id=tx_id,
        )
        example_env.client.update_object(
            "executor",
            executor_id,
            set_updates=[{"path": "/control/touch", "value": {}}],
            transaction_id=tx_id,
        )
        example_env.client.commit_transaction(tx_id)

        timestamp = example_env.client.generate_timestamp()

        response = example_env.client.watch_objects(
            "executor",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=30),
            enable_structured_response=True,
        )
        assert len(response["events"]) == 2
        assert response["events"][0]["event_type"] == "object_updated"
        assert response["events"][1]["event_type"] == "object_created"

    def test_changed_attributes(self, example_env):
        example_config = {
            "watch_manager": {
                "changed_attributes_paths_per_type": {
                    "executor": [
                        "/control",
                        "/control/touch",
                        "/labels/tribe",
                    ]
                },
                "query_selector_enabled": True,
            }
        }
        start_timestamp1 = example_env.client.generate_timestamp()

        executor_id = example_env.client.create_object("executor", request_meta_response=True)

        example_env.client.update_object(
            "executor",
            executor_id,
            set_updates=[{"path": "/labels/tribe", "value": "Khalai"}],
        )
        with example_env.set_cypress_config_patch_in_context(example_config):
            start_timestamp2 = example_env.client.generate_timestamp()
            example_env.client.update_object(
                "executor",
                executor_id,
                set_updates=[{"path": "/labels/tribe", "value": "Khalai1"}],
            )
            example_env.client.update_object(
                "executor", executor_id, set_updates=[{"path": "/control/touch", "value": {}}]
            )
            example_env.client.remove_object("executor", executor_id)
            timestamp = example_env.client.generate_timestamp()

            with pytest.raises(WatchesConfigurationMismatch):
                response = example_env.client.watch_objects(
                    "executor",
                    start_timestamp=start_timestamp1,
                    timestamp=timestamp,
                    time_limit=timedelta(seconds=30),
                    fetch_changed_attributes=True,
                )
            response = example_env.client.watch_objects(
                "executor",
                start_timestamp=start_timestamp2,
                timestamp=timestamp,
                time_limit=timedelta(seconds=30),
                fetch_changed_attributes=True,
            )
            assert len(response["events"]) == 3
            assert "changed_attributes_summary" not in response["events"][2]

            path_to_index = response["changed_attributes_index"]

            def find_index(selector):
                return path_to_index.index({"attribute_path": selector})

            summary1 = response["events"][0]["changed_attributes_summary"]
            summary2 = response["events"][1]["changed_attributes_summary"]

            labels_index = find_index("/labels/tribe")
            assert sum(map(lambda c: c == "t", summary1)) == 1 and summary1[labels_index] == "t"
            assert sum(map(lambda c: c == "t", summary2)) == 2 and summary2[labels_index] == "f"

    def test_watch_log_profiler(self, example_env):
        def _sum_up_per_tablet_lags(sensors):
            return sum(
                [
                    int(sensor["value"]) if "tablet_index" in sensor["tags"] else 0
                    for sensor in sensors
                ]
            )

        leader_fqdn = get_leader_fqdn(example_env.yt_client, "//home/example/master", "Example master")
        profiler = Profiler(
            example_env.yt_client,
            "//home/example/master/instances/{}/orchid/sensors".format(leader_fqdn),
            fixed_tags={
                "object_type": "executor",
                "consumer": "watch_log_consumer1",
                "log_name": "watch_log",
            },
        )

        start_timestamp = example_env.client.generate_timestamp()
        executor_ids = [
            example_env.client.create_object("executor", request_meta_response=True) for _ in range(10)
        ]
        end_timestamp = example_env.client.generate_timestamp()

        result = example_env.client.watch_objects(
            "executor",
            start_timestamp=start_timestamp,
            event_count_limit=5,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=10),
            enable_structured_response=True,
        )
        example_env.client.create_object(
            "watch_log_consumer",
            attributes={
                "meta": {"id": "watch_log_consumer1"},
                "spec": {"object_type": "executor"},
            },
        )
        assert profiler.get_all("objects/watch/lag") == []

        example_env.client.update_object(
            "watch_log_consumer",
            "watch_log_consumer1",
            set_updates=[
                {"path": "/status", "value": {"continuation_token": result["continuation_token"]}}
            ],
        )
        wait(lambda: _sum_up_per_tablet_lags(profiler.get_all("objects/watch/lag")) == 5)

        example_env.client.update_object(
            "executor",
            executor_ids[5],
            set_updates=[{"path": "/labels/tribe", "value": "Khalai"}],
        )
        end_timestamp = example_env.client.generate_timestamp()
        wait(lambda: _sum_up_per_tablet_lags(profiler.get_all("objects/watch/lag")) == 6)

        result_after_update = example_env.client.watch_objects(
            "executor",
            continuation_token=result["continuation_token"],
            event_count_limit=5,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=10),
            enable_structured_response=True,
        )
        example_env.client.update_object(
            "watch_log_consumer",
            "watch_log_consumer1",
            set_updates=[
                {
                    "path": "/status",
                    "value": {"continuation_token": result_after_update["continuation_token"]},
                }
            ],
        )
        wait(lambda: _sum_up_per_tablet_lags(profiler.get_all("objects/watch/lag")) == 1)

    def test_skip_store_without_changes(self, example_env):
        start_timestamp = example_env.client.generate_timestamp()

        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)

        def update_book():
            example_env.client.update_object(
                "book",
                format_object_key(book),
                set_updates=[
                    {"path": "/spec/font", "value": "Garamond"},
                    {"path": "/spec/chapter_descriptions", "value": [{"name": "1"}]},
                ],
            )

        update_book()

        def get_book_spec_timestamp():
            response = example_env.client.get_object(
                "book",
                format_object_key(book),
                selectors=["/spec"],
                options=dict(fetch_timestamps=True, fetch_values=False),
                enable_structured_response=True,
            )
            return response["result"][0]["timestamp"]

        timestamp = example_env.client.generate_timestamp()

        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
        )
        assert len(result) == 2
        assert result[0]["event_type"] == "object_created"
        assert result[1]["event_type"] == "object_updated"

        spec_timestamp_before_update = get_book_spec_timestamp()

        update_book()

        spec_timestamp_after_update = get_book_spec_timestamp()

        assert spec_timestamp_before_update == spec_timestamp_after_update

        end_timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=timestamp,
            timestamp=end_timestamp,
            time_limit=timedelta(seconds=10),
        )
        # NB: empty update still produces a record in watch log
        # assert len(result) == 0

    def test_tags_watch_object_returns_changed_tags(
        self, example_env
    ):
        start_timestamp = example_env.client.generate_timestamp()

        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_rating", "value": 4.95},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
        )
        assert len(result) == 2
        assert result[1]["changed_tags"] == [
            schema_pb2.ETag.TAG_SPEC,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_DIGITAL_DATA,
        ]

    def test_tags_tagged_watchlog_with_required_with_excluded(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_price", "value": 3000},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            watch_log="tagged_watch_log",
        )
        assert len(result) == 0

    def test_tags_tagged_watchlog_without_required(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/name", "value": "Learning Python"},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            watch_log="tagged_watch_log",
        )
        assert len(result) == 0

    def test_tags_tagged_watchlog_with_required(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_rating", "value": 3.7},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            watch_log="tagged_watch_log",
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_SPEC,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_DIGITAL_DATA,
        ]

    def test_root_tags_propagated(
        self, example_env
    ):
        employer = "root_tags_author"
        example_env.client.create_object(
            "employer",
            {"meta": {"id": employer}},
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["id"]
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "employer",
            format_object_key(employer),
            set_updates=[
                {"path": "/meta/email", "value": "employer@yandex-team.ru"},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "employer",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            watch_log="watch_log",
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_META,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_TEAL,
        ]

    def test_tags_tagged_watchlog_multiple_attributes(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_price", "value": 3000},
                {"path": "/spec/keywords", "value": ["nonfiction"]},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            watch_log="tagged_watch_log",
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_SPEC,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_DIGITAL_DATA,
            schema_pb2.ETag.TAG_MONEY,
        ]

    def test_tags_tagged_watch_objects_with_required_with_excluded(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_price", "value": 3000},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            required_tags=[schema_pb2.ETag.TAG_DIGITAL_DATA],
            excluded_tags=[schema_pb2.ETag.TAG_MONEY],
        )
        assert len(result) == 0

    def test_tags_tagged_watch_objects_without_required(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/name", "value": "Learning Python"},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            required_tags=[schema_pb2.ETag.TAG_DIGITAL_DATA],
            excluded_tags=[schema_pb2.ETag.TAG_MONEY],
        )
        assert len(result) == 0

    def test_tags_tagged_watch_objects_with_required(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_rating", "value": 3.7},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            required_tags=[schema_pb2.ETag.TAG_DIGITAL_DATA],
            excluded_tags=[schema_pb2.ETag.TAG_MONEY],
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_SPEC,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_DIGITAL_DATA,
        ]

    def test_tags_tagged_watch_objects_multiple_attributes(
        self, example_env
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_price", "value": 3000},
                {"path": "/spec/keywords", "value": ["fiction"]},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            required_tags=[schema_pb2.ETag.TAG_DIGITAL_DATA],
            excluded_tags=[schema_pb2.ETag.TAG_MONEY],
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_SPEC,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_DIGITAL_DATA,
            schema_pb2.ETag.TAG_MONEY,
        ]

    @pytest.mark.xfail
    def test_tags_request_unknown_tag(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {"path": "/spec/digital_data/store_price", "value": 3000},
                {"path": "/spec/keywords", "value": ["fiction"]},
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            required_tags=[11],
        )

    def test_tags_repeated(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[
                {
                    "path": "/spec",
                    "value": {
                        "chapter_descriptions": [{"name": "Chapter I"}],
                        "digital_data": {
                            "available_formats": [{"format": "pdf", "size": 1024 * 1024 * 3}]
                        },
                    },
                },
            ],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_SPEC,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_PUBLIC_DATA,
            schema_pb2.ETag.TAG_DIGITAL_DATA,
        ]

    @pytest.mark.parametrize(
        "set_update",
        [
            {
                "path": "/spec",
                "value": {
                    "chapter_descriptions": [{"name": "Chapter I"}],
                    "digital_data": {
                        "available_formats": [{"format": "pdf", "size": 1024 * 1024 * 3}]
                    },
                },
            },
            {"path": "/spec/digital_data/store_price", "value": 3000},
        ],
        ids=["whole_spec", "single_attribute_field"],
    )
    def test_tags_no_linear_lookups(
        self, example_env, set_update
    ):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        result = example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[set_update],
            common_options={"fetch_performance_statistics": True},
        )
        assert result["performance_statistics"]["read_phase_count"] == 4

    def test_tags_touch_tag(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[{"path": "/control/touch", "value": {}}],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_TOUCH,
            schema_pb2.ETag.TAG_CONTROL,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_BOOK_MODERATION,
        ]

    @pytest.mark.parametrize("updated_objects_count", [1000])
    def test_read_by_start_timestamp(
        self, example_env, updated_objects_count
    ):
        publisher = example_env.create_publisher()
        books = [example_env.create_book(publisher) for _ in range(updated_objects_count)]
        transaction_id = example_env.client.start_transaction()
        subrequests = [
            {
                "object_type": "book",
                "object_id": books[i],
                "set_updates": [{"path": "/control/touch", "value": {}}],
            }
            for i in range(updated_objects_count)
        ]
        example_env.client.update_objects(subrequests, transaction_id=transaction_id)
        commit_timestamp = example_env.client.commit_transaction(transaction_id)["commit_timestamp"]
        timestamp = example_env.client.generate_timestamp()
        assert 0 == len(
            example_env.client.watch_objects(
                "book",
                start_timestamp=commit_timestamp,
                timestamp=timestamp,
                time_limit=timedelta(seconds=10),
            )
        )

    def test_tags_watch_objects_touch_tag(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            format_object_key(book),
            set_updates=[{"path": "/control/touch", "value": {}}],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
            required_tags=[schema_pb2.ETag.TAG_TOUCH],
        )
        assert len(result) == 1
        assert result[0]["event_type"] == "object_updated"
        assert result[0]["changed_tags"] == [
            schema_pb2.ETag.TAG_TOUCH,
            schema_pb2.ETag.TAG_CONTROL,
            schema_pb2.ETag.TAG_ANY,
            schema_pb2.ETag.TAG_BOOK_MODERATION,
        ]

    def test_watch_log_read_time_limit(self, example_env):
        config = dict(
            watch_manager=dict(
                allow_filtering_by_meta=True,
                query_filter_enabled=True,
                labels_store_enabled=True,
                per_tablet_select_limit=10,
            )
        )
        with example_env.set_cypress_config_patch_in_context(config):
            executor = example_env.client.create_object("executor")
            start_timestamp = example_env.client.generate_timestamp()
            for i in range(15):
                example_env.client.update_object(
                    "executor",
                    str(executor),
                    set_updates=[
                        {"path": "/labels/tribe", "value": f"tribe_{i}"},
                    ],
                )
            example_env.client.update_object(
                "executor",
                str(executor),
                set_updates=[
                    {"path": "/labels/tribe", "value": "test"},
                ],
            )
            timestamp = example_env.client.generate_timestamp()
            result = example_env.client.watch_objects(
                "executor",
                start_timestamp=start_timestamp,
                timestamp=timestamp,
                time_limit=timedelta(seconds=10),
                read_time_limit=timedelta(microseconds=1),
                event_count_limit=1,
                watch_log="selector_watch_log",
                filter="[/labels/tribe] = 'test'",
                enable_structured_response=True,
            )
            assert 0 == len(result.get("events", ()))
            continuation_token = result["continuation_token"]
            assert continuation_token

            result = example_env.client.watch_objects(
                "executor",
                timestamp=timestamp,
                time_limit=timedelta(seconds=10),
                event_count_limit=10,
                watch_log="selector_watch_log",
                filter="[/labels/tribe] = 'test'",
                continuation_token=continuation_token,
            )

            assert len(result) == 1
            assert result[0]["event_type"] == "object_updated"

    @pytest.mark.parametrize(
        "skip_trimmed,expectation",
        [(False, pytest.raises(RowsAlreadyTrimmedError)), (True, does_not_raise())],
    )
    def test_trimmed_rows(self, example_env, skip_trimmed, expectation):
        client = example_env.client
        start_timestamp = client.generate_timestamp()
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)

        def touch():
            client.update_object(
                "book",
                format_object_key(book),
                set_updates=[{"path": "/control/touch", "value": {}}],
            )

        touch()

        example_env.db_manager.trim_table("books_watch_log")

        touch()
        touch()

        timestamp = client.generate_timestamp()
        with expectation:
            client.watch_objects(
                "book",
                timestamp=timestamp,
                time_limit=timedelta(seconds=10),
                start_timestamp=start_timestamp,
                skip_trimmed=skip_trimmed,
            )

    def test_created_and_removed_object_is_transparent(self, example_env):
        publisher = example_env.create_publisher()
        client = example_env.client
        timestamp = client.generate_timestamp()
        transaction_id = client.start_transaction()
        book = client.create_object(
            "book",
            attributes={
                "meta": {
                    "isbn": "978-1449355739",
                    "parent_key": str(publisher),
                },
            },
            transaction_id=transaction_id,
        )
        client.remove_object(
            "book",
            str(book),
            transaction_id=transaction_id
        )
        client.commit_transaction(transaction_id)

        assert 0 == len(client.watch_objects(
            "book",
            timestamp=client.generate_timestamp(),
            time_limit=timedelta(seconds=10),
            start_timestamp=timestamp,
        ))

        assert 0 == len(client.select_object_history(
            "book",
            str(book),
            selectors=["/meta"],
        )["events"])

    def test_tablet_expansion(self, example_env):
        start_timestamp = example_env.client.generate_timestamp()
        tablet_count = example_env.yt_client.get("//home/example/db/executors_watch_log/@tablet_count")
        example_env.client.create_objects((("executor", dict()) for _ in range(tablet_count)))
        event_count = tablet_count

        def _at_least_one_event_in_tablet(tablet):
            result = example_env.client.watch_objects(
                "executor",
                tablets=[tablet],
                start_timestamp=start_timestamp,
                timestamp=example_env.client.generate_timestamp(),
                time_limit=timedelta(seconds=10),
            )
            return len(result) > 0

        def _at_least_one_event_in_all_tablets():
            return all(map(_at_least_one_event_in_tablet, range(tablet_count)))

        def _add_event():
            nonlocal event_count
            example_env.client.create_object("executor")
            event_count += 1

        while not _at_least_one_event_in_all_tablets():
            _add_event()

        result = example_env.client.watch_objects(
            "executor",
            start_timestamp=start_timestamp,
            time_limit=timedelta(seconds=10),
            timestamp=example_env.client.generate_timestamp(),
            enable_structured_response=True,
        )
        assert len(result["events"]) == event_count

        tablet_count += 1
        example_env.db_manager.reshard_table("executors_watch_log", tablet_count)

        while not _at_least_one_event_in_tablet(tablet_count - 1):
            _add_event()

        events = example_env.client.watch_objects(
            "executor",
            continuation_token=result["continuation_token"],
            timestamp=example_env.client.generate_timestamp(),
            time_limit=timedelta(seconds=10),
        )
        assert len(events) + len(result["events"]) == event_count
