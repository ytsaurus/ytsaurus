from .conftest import ExampleTestEnvironment, HistoryMigrationState, create_user_client

from yt.orm.tests.helpers import assert_over_time

from yt.orm.library.common import YtResponseError, InvalidContinuationTokenError

from yt.common import datetime_to_string

import typing as tp
import datetime
import pytest
import time


class TestHistoryApi:
    def test_views(self, example_env, for_both_history_tables):
        illustrator = example_env.create_illustrator()
        example_env.client.update_object(
            "illustrator",
            str(illustrator),
            set_updates=[{"path": "/meta/name", "value": "John"}]
        )

    def test_full_object_history(self, example_env, for_both_history_tables):
        employer = example_env.client.create_object("employer", attributes=dict(meta=dict(id="Yandex")))
        example_env.client.update_object(
            "employer",
            employer,
            set_updates=[{"path": "/labels/a", "value": "b", "recursive": True}],
        )

        result = example_env.client.select_object_history("employer", employer, [""])
        assert len(result["events"]) == 2
        assert result["events"][0]["results"][0]["value"]["labels"] == dict()
        assert result["events"][1]["results"][0]["value"]["labels"] == dict(a="b")
        assert result["events"][1]["results"][0]["value"]["meta"]["id"] == "Yandex"

    def test_fetch_root(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        client = example_env.client
        employer = client.create_object("employer", attributes={"meta": {"id": "0"}, "status": {"job_title": "baker"}})
        events = client.select_object_history("employer", str(employer), ["/meta/id", "/status/job_title"], options={"fetch_root_object": True})["events"]
        assert 1 == len(events)
        assert 1 == len(events[0]["results"])
        payload = events[0]["results"][0]["value"]
        assert "0" == payload["meta"]["id"]
        assert "baker" == payload["status"]["job_title"]

    def test_fetch_root_empty_selector(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        book = example_env.create_book()
        events = example_env.client.select_object_history("book", str(book), selectors=[], options={"fetch_root_object": True})["events"]
        assert 1 == len(events)
        results = events[0]["results"]
        assert 1 == len(results)
        assert {} == results[0]["value"]

    def test_trim_history(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        start_time = time.time()
        publisher_identity = example_env.client.create_object("publisher")
        time.sleep(1)
        trim_time = time.time()
        time.sleep(1)
        after_trim_time = time.time()
        example_env.client.update_object(
            "publisher",
            publisher_identity,
            set_updates=[{"path": "/control/touch", "value": {"store_event_to_history": True}}],
        )

        def does_not_raise(time):
            try:
                result = example_env.client.select_object_history(
                    "publisher",
                    publisher_identity,
                    ["/control/touch"],
                    options=dict(
                        interval=(int(time * 1e6) if time else None, None),
                        allow_time_mode_conversion=True,
                    ),
                )
            except YtResponseError:
                return False, None
            return True, result

        assert does_not_raise(start_time)[0]

        trim_time_string = datetime_to_string(
            datetime.datetime.fromtimestamp(trim_time), is_local=True
        )
        history_table_name = for_both_history_tables[0]
        example_env.dynamic_config.update_config(
            {
                "object_manager": {
                    "last_trim_time_per_history_table": {
                        history_table_name: trim_time_string
                    }
                }
            }
        )

        assert not does_not_raise(start_time)[0]
        for request_time in (None, after_trim_time):
            result = does_not_raise(request_time)
            assert result[0]
            assert 1 == len(result[1]["events"])

    def test_touch_history(self, example_env, for_both_history_tables):
        publisher_identity = example_env.client.create_object("publisher")

        def update_object(value):
            example_env.client.update_object(
                "publisher",
                publisher_identity,
                set_updates=[{"path": "/control/touch", "value": value}],
            )

        update_object({})
        update_object({"store_event_to_history": True})
        update_object({"store_event_to_history": False})
        time.sleep(0.5)
        actual = example_env.client.select_object_history(
            "publisher", publisher_identity, ["/control/touch"]
        )
        assert 2 == len(actual["events"])
        assert actual["events"][0]["event_type"] == 1
        assert actual["events"][1]["event_type"] == 3

    def test_uuid(self, example_env, for_both_history_tables):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        book_id = int(book.split(';')[0])
        book_id2 = int(book.split(';')[1])
        uuid = example_env.client.get_object("book", str(book), ["/meta/uuid"])[0]

        def update_book(**kwargs):
            return example_env.client.update_object("book", str(book), **kwargs)

        update_book(
            set_updates=[
                {
                    "path": "/spec/digital_data/available_formats",
                    "value": [{"format": "unknown", "size": 64}],
                    "recursive": True,
                }
            ]
        )
        update_book(set_updates=[{"path": "/spec/name", "value": "The book of dragons"}])
        update_book(
            set_updates=[{"path": "/spec/design/cover/hardcover", "value": True, "recursive": True}]
        )
        update_book(remove_updates=[{"path": "/spec/digital_data/available_formats"}])

        result = example_env.client.select_object_history(
            "book", str(book), ["/spec/digital_data/available_formats"]
        )
        result_with_uuid = example_env.client.select_object_history(
            "book", str(book), ["/spec/digital_data/available_formats"], options=dict(uuid=uuid)
        )
        result_with_distinct = example_env.client.select_object_history(
            "book", str(book), ["/spec/digital_data/available_formats"], options=dict(distinct=True)
        )
        assert 4 == len(result["events"])
        assert result["events"] == result_with_uuid["events"]
        assert 3 == len(result_with_distinct["events"])

        timestamp = example_env.client.generate_timestamp()
        tx_id = example_env.client.start_transaction(start_timestamp=timestamp)
        example_env.client.remove_object("book", str(book), transaction_id=tx_id)
        example_env.client.create_object(
            "book",
            dict(meta=dict(id=book_id, id2=book_id2, parent_key=str(publisher), isbn="978-1449355739")),
            transaction_id=tx_id,
        )
        example_env.client.commit_transaction(tx_id)

        update_book(set_updates=[{"path": "/spec/name", "value": "The book of dragons"}])
        result = example_env.client.select_object_history(
            "book",
            str(book),
            ["/spec/digital_data/available_formats"],
            options=dict(continuation_token=result["continuation_token"]),
        )
        assert 2 == len(result["events"])
        assert result["events"][0]["event_type"] == 2
        assert result["events"][1]["event_type"] == 1

        result = example_env.client.select_object_history(
            "book",
            str(book),
            ["/spec/digital_data/available_formats"],
            options=dict(descending_time_order=True, limit=2),
        )
        assert 2 == len(result["events"])
        assert result["events"][0]["event_type"] == 1
        assert result["events"][1]["event_type"] == 2

        with pytest.raises(InvalidContinuationTokenError):
            result_with_uuid = example_env.client.select_object_history(
                "book",
                str(book),
                ["/spec/digital_data/available_formats"],
                options=dict(uuid=uuid, continuation_token=result["continuation_token"]),
            )
        result_with_uuid = example_env.client.select_object_history(
            "book",
            str(book),
            ["/spec/digital_data/available_formats"],
            options=dict(uuid=uuid, continuation_token=result_with_uuid["continuation_token"]),
        )
        assert 1 == len(result_with_uuid["events"])

    def test_start_time(self, example_env, for_both_history_tables):
        start_time = int(time.time() * 1e6)
        start_response = example_env.client.start_transaction(enable_structured_response=True)
        assert "start_timestamp" in start_response
        event_timestamp = start_response["start_timestamp"]

        publisher_identity = example_env.client.create_object(
            "publisher",
            request_meta_response=True,
            transaction_id=start_response["transaction_id"],
        )
        example_env.client.commit_transaction(start_response["transaction_id"])
        end_time = int(time.time() * 1e6)
        time.sleep(1)
        example_env.client.update_object(
            "publisher", publisher_identity, set_updates=[dict(path="/spec/name", value="O'Reilly")]
        )

        actual = example_env.client.select_object_history(
            "publisher", publisher_identity, ["/spec/illustrator_in_chief"]
        )
        assert 2 == len(actual["events"])
        assert actual["events"][0]["timestamp"] == event_timestamp

        assert 2 == len(
            example_env.client.select_object_history(
                "publisher",
                publisher_identity,
                ["/spec/illustrator_in_chief"],
                options=dict(timestamp_interval=[event_timestamp, None]),
            )["events"]
        )
        assert 0 == len(
            example_env.client.select_object_history(
                "publisher",
                publisher_identity,
                ["/spec/illustrator_in_chief"],
                options=dict(timestamp_interval=[None, event_timestamp]),
            )["events"]
        )
        with pytest.raises(YtResponseError):
            example_env.client.select_object_history(
                "publisher",
                publisher_identity,
                ["/spec/illustrator_in_chief"],
                options=dict(interval=[start_time, None]),
            )
        result = example_env.client.select_object_history(
            "publisher",
            publisher_identity,
            ["/spec/illustrator_in_chief"],
            options=dict(interval=[start_time, end_time], allow_time_mode_conversion=True),
        )["events"]
        assert 1 == len(result)
        assert "time" in result[0]
        result = example_env.client.select_object_history(
            "publisher",
            publisher_identity,
            ["/spec/illustrator_in_chief"],
            options=dict(interval=[start_time, None], allow_time_mode_conversion=True),
        )["events"]
        del result[0]["time"]
        del result[1]["time"]
        assert result == actual["events"]

    def test_force_allow_time_mode_conversion(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        client = example_env.client
        publisher = example_env.create_publisher()

        def select_publisher_creation():
            events = client.select_object_history("publisher", str(publisher), ["/meta/id"])["events"]
            assert 1 == len(events)
            return events[0]

        example_env.dynamic_config.update_config({
            "transaction_manager": {"select_object_history": {"force_allow_time_mode_conversion": True}}
        })
        assert "time" in select_publisher_creation()

        example_env.dynamic_config.update_config({
            "transaction_manager": {"select_object_history": {"force_allow_time_mode_conversion": False}}
        })
        assert "time" not in select_publisher_creation()

    def test_etc_attributes_change(self, example_env, for_both_history_tables):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)

        example_env.client.update_object(
            "book",
            str(book),
            set_updates=[
                {"path": "/spec/design/cover/image", "value": "<image>", "recursive": True}
            ],
        )
        example_env.client.update_object(
            "book",
            str(book),
            set_updates=[{"path": "/spec/page_count", "value": 42, "recursive": True}],
        )
        # Sleep between 2 consequent updates, so that event.time would be different.
        time.sleep(1)
        tx_start_response = example_env.client.start_transaction(enable_structured_response=True)
        example_env.client.update_object(
            "book",
            str(book),
            set_updates=[{"path": "/spec/design/cover/hardcover", "value": True}],
            transaction_id=tx_start_response["transaction_id"],
        )
        example_env.client.commit_transaction(tx_start_response["transaction_id"])

        result = example_env.client.select_object_history("book", str(book), ["/spec/design/cover"])

        assert 2 == len(result["events"])
        assert result["events"][1]["timestamp"] == tx_start_response["start_timestamp"]

    def test_history_for_disabled_type(self, example_env, for_both_history_tables):
        example_env.client.create_object("nexus")
        nexus = example_env.client.client_traits.object_type_enum_type.OT_NEXUS

        def get_rows():
            result = example_env.yt_client.select_rows(
                f"1 FROM [//home/example/db/{for_both_history_tables[0]}] WHERE object_type = {nexus}"
            )
            return list(result)

        assert_over_time(lambda: len(get_rows()) == 0)

    def test_distinct_descending_time(self, example_env):
        client = example_env.example_client

        book = example_env.create_book(spec={"font": "Times", "year": 1999})
        client.update_object("book", book, set_updates=[{"path": "/spec/year", "value": 2001}])

        def select_history_events(descending_time_order=False):
            return client.select_object_history(
                "book",
                book,
                ["/spec/font"],
                options={"distinct": True, "descending_time_order": descending_time_order},
            )["events"]

        assert 1 == len(select_history_events())
        assert 1 == len(select_history_events(descending_time_order=True))

    @pytest.mark.parametrize(
        "skip,superuser,has_permission,skipped",
        [
            (True, True, False, True),
            (True, False, True, True),
            (True, False, False, False),
            (False, True, True, False),
        ],
    )
    def test_skip_history_option(
        self,
        skip,
        superuser,
        has_permission,
        skipped,
        example_env,
        regular_user1,
        for_both_history_tables
    ):
        client = example_env.client
        if not superuser:
            client = regular_user1.client
            if has_permission:
                example_env.grant_permission(
                    "schema",
                    "publisher",
                    "use",
                    regular_user1.id,
                    "/access/event_generation_skip_allowed",
                )
            example_env.grant_permission("schema", "publisher", "create", regular_user1.id, "")

        start_timestamp = client.generate_timestamp()

        create_transaction = client.start_transaction(skip_history=skip)
        publisher_id = client.create_object("publisher", transaction_id=create_transaction)
        client.commit_transaction(create_transaction)

        update_transaction = client.start_transaction(skip_history=skip)
        client.update_object(
            "publisher", publisher_id, [{"path": "/spec/name", "value": "Henry"}], transaction_id=update_transaction
        )
        client.commit_transaction(update_transaction)

        remove_transaction = client.start_transaction(skip_history=skip)
        client.remove_object("publisher", publisher_id, transaction_id=remove_transaction)
        client.commit_transaction(remove_transaction)

        end_timestamp = client.generate_timestamp()
        result = client.select_object_history(
            "publisher",
            publisher_id,
            [""],
            options=dict(timestamp_interval=[start_timestamp, end_timestamp])
        )

        if skipped:
            assert len(result["events"]) == 0
        else:
            assert len(result["events"]) != 0

    def test_trace_id(self, example_env: ExampleTestEnvironment):
        client = example_env.client
        book = example_env.create_book()
        example_env.append_book_available_format(book)

        def select_book_history(trace_id=None):
            options = dict()
            if trace_id is not None:
                options["filter"] = f"[/event/transaction_context/items/trace_id] = '{trace_id}'"

            return client.select_object_history("book", str(book), ["/meta/id"], options)["events"]

        events = select_book_history()
        assert 2 == len(events)
        create_trace_id = events[0]["transaction_context"]["items"]["trace_id"]
        update_trace_id = events[0]["transaction_context"]["items"]["trace_id"]

        events = select_book_history(create_trace_id)
        assert 1 == len(events)
        events[0]["transaction_context"]["items"]["trace_id"] == create_trace_id

        events = select_book_history(update_trace_id)
        assert 1 == len(events)
        events[0]["transaction_context"]["items"]["trace_id"] == update_trace_id


class TestDistinctBy:
    @pytest.fixture
    def update_book(self, example_env):
        def _inner(book, set_updates):
            return example_env.client.update_object("book", str(book), set_updates)

        return _inner

    @pytest.fixture
    def select_book_history(self, example_env):
        def _inner(book, start_timestamp, distinct_by=None, index_mode="default"):
            end_timestamp = example_env.client.generate_timestamp()
            return example_env.client.select_object_history(
                "book",
                str(book),
                ["/spec"],
                options={
                    "distinct": True,
                    "distinct_by": distinct_by,
                    "timestamp_interval": [start_timestamp, end_timestamp],
                    "index_mode": index_mode,
                },
            )

        return _inner

    def test_history_distinct_by(self, example_env, select_book_history, update_book, for_both_history_tables):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()

        update_book(book, [{"path": "/spec/font", "value": "Times"}, {"path": "/spec/year", "value": 1998}])
        assert 1 == len(select_book_history(book, start_timestamp)["events"])

        update_book(book, [{"path": "/spec/font", "value": "Times"}, {"path": "/spec/year", "value": 1998}])
        assert 1 == len(select_book_history(book, start_timestamp)["events"])

        update_book(book, [{"path": "/spec/font", "value": "Times"}, {"path": "/spec/year", "value": 1999}])
        assert 2 == len(select_book_history(book, start_timestamp)["events"])
        assert 1 == len(select_book_history(book, start_timestamp, distinct_by=["/spec/font"])["events"])
        assert 2 == len(select_book_history(book, start_timestamp, distinct_by=["/spec/year"])["events"])

    def test_indexed_distinct_by(self, example_env, select_book_history, update_book, for_both_history_tables):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        start_timestamp = example_env.client.generate_timestamp()

        update_book(book, [{"path": "/spec/font", "value": "Times"}, {"path": "/spec/year", "value": 1998}])
        assert 1 == len(
            select_book_history(
                book, start_timestamp, index_mode="enabled", distinct_by=["/spec/digital_data/available_formats"]
            )["events"]
        )

        update_book(book, [{"path": "/spec/font", "value": "Times"}, {"path": "/spec/year", "value": 1999}])
        assert 1 == len(
            select_book_history(
                book, start_timestamp, index_mode="enabled", distinct_by=["/spec/digital_data/available_formats"]
            )["events"]
        )

        update_book(
            book,
            [
                {"path": "/spec/digital_data/available_formats", "value": [{"format": "epub"}]},
            ],
        )
        assert 2 == len(
            select_book_history(
                book, start_timestamp, index_mode="enabled", distinct_by=["/spec/digital_data/available_formats"]
            )["events"]
        )

    def test_uuid_in_index(self, example_env: ExampleTestEnvironment, for_both_history_tables):
        book_meta = example_env.create_book(enable_structured_response=True)["meta"]
        events = example_env.client.select_object_history(
            "book",
            str(book_meta["key"]),
            ["/spec"],
            options={
                "uuid": book_meta["uuid"],
                "distinct": True,
                "distinct_by": ["/spec/digital_data/available_formats"],
                "index_mode": "enabled",
            },
        )["events"]
        assert 1 == len(events)


class TestFilter:
    def test_simple(self, example_env, for_both_history_tables):
        client = example_env.client
        author = example_env.create_author()
        client.update_object("author", author, set_updates=[{"path": "/spec/age", "value": 18}])
        response = client.start_transaction(enable_structured_response=True)
        first_ts = response["start_timestamp"]

        client.update_object(
            "author",
            author,
            set_updates=[{"path": "/spec/age", "value": 19}],
            transaction_id=response["transaction_id"]
        )
        client.commit_transaction(response["transaction_id"])

        client.update_object("author", author, set_updates=[{"path": "/spec/age", "value": 20}])

        response = client.start_transaction(enable_structured_response=True)
        second_ts = response["start_timestamp"]
        client.update_object(
            "author",
            author,
            set_updates=[{"path": "/spec/age", "value": 19}],
            transaction_id=response["transaction_id"]
        )
        client.commit_transaction(response["transaction_id"])

        client.remove_object("author", author)

        result = client.select_object_history(
            "author", author, ["/spec/age"], options=dict(filter="int64([/spec/age]) = 19")
        )
        assert len(result["events"]) == 2
        assert result["events"][0]["results"][0]["value"] == 19
        assert result["events"][0]["timestamp"] == first_ts
        assert result["events"][1]["results"][0]["value"] == 19
        assert result["events"][1]["timestamp"] == second_ts

    def test_distinct(self, example_env, for_both_history_tables):
        client = example_env.client
        author = example_env.create_author(age=18)
        client.update_object("author", author, set_updates=[{"path": "/spec/age", "value": 18}])
        client.update_object("author", author, set_updates=[
            dict(path="/spec/age", value=18),
            dict(path="/spec/name", value="Ivan"),
        ])

        client.update_object("author", author, set_updates=[{"path": "/spec/age", "value": 19}])
        client.update_object("author", author, set_updates=[{"path": "/spec/age", "value": 19}])

        client.remove_object("author", author)
        result = client.select_object_history(
            "author", author, ["/spec/age"], options=dict(filter="int64([/spec/age]) = 18")
        )
        assert len(result["events"]) == 2

        result = client.select_object_history(
            "author", author, ["/spec/age"], options=dict(filter="int64([/spec/age]) = 18", distinct=True)
        )
        assert len(result["events"]) == 1
        assert result["events"][0]["event_type"] == 1

    def test_index(self, example_env, for_both_history_tables):
        client = example_env.client
        formats = [
            dict(format="pdf", size=64),
            dict(format="unknown", size=128),
        ]
        book = str(example_env.create_book(spec=dict(
            digital_data=dict(
                available_formats=formats,
            ),
        )))
        client.update_object(
            "book", book, set_updates=[{"path": "/spec/digital_data/available_formats", "value": formats}]
        )

        response = client.start_transaction(enable_structured_response=True)
        ts = response["start_timestamp"]
        client.update_object(
            "book", book,
            set_updates=[{"path": "/spec/digital_data/available_formats/1/format", "value": "pdf"}],
            transaction_id=response["transaction_id"]
        )
        client.commit_transaction(response["transaction_id"])

        client.update_object(
            "book", book, set_updates=[{"path": "/spec/digital_data/available_formats/0/format", "value": "unknown"}]
        )
        client.update_object(
            "book", book, set_updates=[{"path": "/spec/digital_data/available_formats/0/format", "value": "pdf"}]
        )
        result = client.select_object_history(
            "book",
            book,
            ["/spec/digital_data/available_formats"],
            options=dict(
                distinct=True,
                filter="[/spec/digital_data/available_formats/0/format] = 1",
                index_mode="enabled",
            )
        )
        assert len(result["events"]) == 2
        assert result["events"][0]["event_type"] == 1
        assert result["events"][1]["timestamp"] == ts


class TestHistoryFilter:
    def test_employer(self, example_env, for_both_history_tables):
        employer = example_env.client.create_object("employer", {
            "meta": {"id": "employer1"},
            "labels": {"skip_history": True},
        })
        assert 0 == len(example_env.client.select_object_history("employer", str(employer), [""])["events"])

        example_env.client.update_object(
            "employer",
            employer,
            set_updates=[
                {
                    "path": "/labels/skip_history",
                    "value": False,
                },
            ],
        )
        assert 1 == len(example_env.client.select_object_history("employer", str(employer), [""])["events"])

    def test_publisher(self, example_env, for_both_history_tables):
        publisher = example_env.create_publisher(name="Secret Publisher")
        assert 0 == len(example_env.client.select_object_history("publisher", str(publisher), [""])["events"])

        example_env.update_publisher_name(publisher, "Public Publishers Society")
        assert 1 == len(example_env.client.select_object_history("publisher", str(publisher), [""])["events"])

    def test_by_user_filter(self, test_environment, example_env, for_both_history_tables):
        publisher = example_env.create_publisher(name="Secret Publisher")
        assert 0 == len(example_env.client.select_object_history("publisher", str(publisher), [""])["events"])

        with create_user_client(test_environment) as client:
            client_id = client.get_user_id()
            example_env.client.update_object(
                "publisher",
                str(publisher),
                set_updates=[
                    {
                        "path": "/meta/acl/end",
                        "value": {"action": "allow", "permissions": ["write"], "subjects": [client_id]}
                    },
                ],
            )

            example_env.update_publisher_name(publisher, "Open Publisher", client=client)

            assert 0 == len(client.select_object_history(
                "publisher",
                str(publisher),
                [""],
                options={"filter": "[/event/user] != '{}'".format(client_id)}
            )["events"])

            assert 1 == len(client.select_object_history(
                "publisher",
                str(publisher),
                [""],
                options={"filter": "[/event/user] = '{}'".format(client_id)}
            )["events"])

    def test_by_transaction_id_filter(self, example_env, for_both_history_tables):
        client = example_env.client
        publisher = example_env.create_publisher(name="Secret Publisher")
        assert 0 == len(client.select_object_history("publisher", str(publisher), [""])["events"])

        example_env.update_publisher_name(publisher, "Open Publisher")
        assert 1 == len(client.select_object_history("publisher", str(publisher), [""])["events"])

        transaction_id = client.start_transaction()
        example_env.update_publisher_name(publisher, "New Publisher", transaction_id=transaction_id)
        client.commit_transaction(transaction_id)

        assert 2 == len(client.select_object_history("publisher", str(publisher), [""])["events"])
        assert 1 == len(client.select_object_history(
            "publisher",
            str(publisher),
            [""],
            options={"filter": "[/event/transaction_id] = '{}'".format(transaction_id)}
        )["events"])

        assert 1 == len(client.select_object_history(
            "publisher",
            str(publisher),
            [""],
            options={"filter": "[/event/transaction_id] != '{}'".format(transaction_id)}
        )["events"])


class TestAnnotationsHistory:
    def test_book(self, example_env, for_both_history_tables):
        book = example_env.create_book()

        start_ts = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            str(book),
            set_updates=[
                {
                    "path": "/annotations/rating",
                    "value": 5.0,
                },
            ],
        )

        result = example_env.client.select_object_history(
            "book",
            str(book),
            ["/annotations/rating"],
            options={"timestamp_interval": [start_ts, None]},
        )["events"]
        assert len(result) == 1
        assert result[0]['results'][0]['value'] == 5.0


class TestTokenTransfer:
    @pytest.mark.parametrize("state", [
        pytest.param(
            HistoryMigrationState.INITIAL,
            marks=pytest.mark.xfail(reason="Enabled `use_deprecated_hashing` contains bug in token transfer")),
        HistoryMigrationState.TARGET,
    ])
    def test(self, state: HistoryMigrationState, example_env: ExampleTestEnvironment):
        example_env.set_history_migration_state(state)
        client = example_env.client

        TOTAL_ATTEMPT_COUNT = 10
        for _ in range(TOTAL_ATTEMPT_COUNT):
            book = example_env.create_book()

            transaction_id = client.start_transaction()
            client.remove_object("book", str(book), transaction_id=transaction_id)
            example_env.create_book(book_id=book, transaction_id=transaction_id)
            client.commit_transaction(transaction_id)

            def select_book_history(use_index: bool, limit: int, continuation_token: tp.Optional[str] = None):
                options = {
                    "index_mode": "enabled" if use_index else "disabled",
                    "limit": limit,
                    "distinct": True,
                }

                if continuation_token is not None:
                    options["continuation_token"] = continuation_token

                result = client.select_object_history("book", str(book), ["/spec/digital_data/available_formats"], options)
                return result["events"], result["continuation_token"]

            events, token = select_book_history(use_index=False, limit=1)

            for _ in range(2):
                index_events, _ = select_book_history(use_index=True, limit=1, continuation_token=token)
                events, token = select_book_history(use_index=False, limit=1, continuation_token=token)
                assert events == index_events
