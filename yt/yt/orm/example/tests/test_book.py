from yt.yt.orm.example.client.proto.data_model.autogen import schema_pb2
from yt.orm.library.common import wait
from yt.orm.library.orchid_client import get_leader_fqdn
from yt.orm.tests.base_object_test import BaseObjectTest

from yt.wrapper.errors import YtTabletTransactionLockConflict
from yt.test_helpers.profiler import Profiler

from datetime import timedelta
import pytest


class TestBook(BaseObjectTest):
    @pytest.fixture
    def object_type(self):
        return "book"

    def object_watchable(self):
        return True

    @pytest.fixture
    def object_spec(self, example_env):
        editor_id = example_env.client.create_object("editor", {"spec": {"name": "Greg"}})
        return {
            "name": "Learning Python",
            "year": 2013,
            "font": "Times New Roman",
            "genres": ["education", "programming"],
            "keywords": ["learning", "python"],
            "design": {
                "cover": {
                    "hardcover": True,
                    "image": "http://images.net/path/to/cover.png",
                },
                "color": "white",
                "illustrations": ["img1.jpeg", "img2.jpeg"],
            },
            "editor_id": editor_id,
            "digital_data": {
                "store_rating": 4.9,
                "available_formats": [
                    {
                        "format": 2,
                        "size": 11778382,
                    },
                    {
                        "format": 1,
                        "size": 12778383,
                    },
                ],
            },
            "chapter_descriptions": [],
            "page_count": 256,
            "author_ids": [],
            "illustrator_id": example_env.create_illustrator(name="Melman"),
            "cover_illustrator_id": example_env.create_illustrator(name="Mason"),
            "alternative_publisher_ids": [],
            "peer_review": {
                "reviewer_ids": [],
                "summary": "",
            }
        }

    @pytest.fixture
    def object_meta(self, example_env):
        return {
            "isbn": "978-1449355739",
            "parent_key": example_env.client.create_object(
                "publisher",
                {"spec": {"name": "O'REILLY"}},
                request_meta_response=True,
            ),
        }

    @pytest.fixture
    def set_updates(self):
        return [
            {"path": "/spec/name", "value": "The C++ Programming Language"},
            {"path": "/spec/font", "value": "Garamond"},
        ]

    def history_enabled_selectors(self):
        return ["/spec/font"]

    @pytest.mark.xfail
    def test_touch_index(self, example_env):
        publisher = example_env.client.create_object("publisher")
        book = example_env.client.create_object("book", dict(meta=dict(isbn="978-1449355730", parent_key=publisher)))
        example_env.client.update_object(
            "book", book, set_updates=[dict(path="/control/touch_index", value=dict(index_names=["books_by_authors"]))]
        )

    def test_custom_touch(self, example_env):
        book = example_env.create_book()
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object("book", str(book), set_updates=[{"path": "/control/money_touch", "value": {}}])
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=30),
        )
        assert schema_pb2.ETag.TAG_MONEY in result[0]["changed_tags"]


class TestHashExpressionContinuation:
    def test(self, example_env):
        book_keys = []
        publisher_keys = example_env.client.create_objects(
            (("publisher", dict()) for _ in range(10)),
        )
        # /meta/parent_key and /spec/year must be different for different books to
        # provide non-trivial state for the following continuation.
        book_keys = example_env.client.create_objects(
            (
                ("book", dict(meta=dict(isbn="978-1449355739", parent_key=publisher_key), spec=dict(year=1900 + i)))
                for i, publisher_key in enumerate(publisher_keys)
            ),
        )
        assert 10 == len(set(book_keys))

        limit = 3
        continuation_token = None
        selected_book_keys = []
        while True:
            options = dict()
            if continuation_token is not None:
                options["continuation_token"] = continuation_token
            response = example_env.client.select_objects(
                "book",
                filter="[/spec/year] IN (1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909)",
                selectors=["/meta/key"],
                limit=limit,
                options=options,
                index="books_by_year",
                enable_structured_response=True,
            )
            results = response["results"]
            continuation_token = response["continuation_token"]
            for result in results:
                selected_book_keys.append(result[0]["value"])
            if len(results) < limit:
                break
        assert list(sorted(book_keys)) == list(sorted(selected_book_keys))


class TestConcurrentPublisherRemovalAndBookCreation:
    def test(self, example_env):
        publisher_key = example_env.client.create_object("publisher")

        book_creation = example_env.client.start_transaction()

        publisher_removal = example_env.client.start_transaction()
        example_env.client.remove_object("publisher", publisher_key, transaction_id=publisher_removal)
        example_env.client.commit_transaction(publisher_removal)

        example_env.client.create_object(
            "book",
            {"meta": {"isbn": "978-1449355739", "parent_key": publisher_key}},
            transaction_id=book_creation,
            request_meta_response=True,
        )

        with pytest.raises(YtTabletTransactionLockConflict):
            example_env.client.commit_transaction(book_creation)


class TestAttributeTimestampPrerequisite:
    def test_update(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)

        example_config = {
            "transaction_manager": {
                "attribute_prerequisite_read_lock_enabled": True,
            }
        }
        with example_env.set_cypress_config_patch_in_context(example_config):
            timestamp = example_env.client.generate_timestamp()

            tx_id = example_env.client.start_transaction()
            example_env.client.update_object(
                "book",
                str(book),
                set_updates=[{"path": "/spec/font", "value": "Garamond"}],
                attribute_timestamp_prerequisites=[{"path": "/spec/name", "timestamp": timestamp}],
                transaction_id=tx_id,
            )

            example_env.client.update_object(
                "book",
                str(book),
                set_updates=[{"path": "/spec/name", "value": "The C++ Programming Language"}],
            )

            with pytest.raises(YtTabletTransactionLockConflict):
                example_env.client.commit_transaction(tx_id)

    def test_create_update_existing(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)

        example_config = {
            "transaction_manager": {
                "attribute_prerequisite_read_lock_enabled": True,
            }
        }
        with example_env.set_cypress_config_patch_in_context(example_config):
            timestamp = example_env.client.generate_timestamp()

            tx_id = example_env.client.start_transaction()
            example_env.client.create_object(
                "book",
                attributes={"meta": {"parent_key": str(publisher), "key": str(book)}},
                transaction_id=tx_id,
                update_if_existing={
                    "set_updates": [{"path": "/spec/font", "value": "Garamond"}],
                    "attribute_timestamp_prerequisites": [{"path": "/spec/name", "timestamp": timestamp}],
                },
            )

            example_env.client.update_object(
                "book",
                str(book),
                set_updates=[{"path": "/spec/name", "value": "The C++ Programming Language"}],
            )

            with pytest.raises(YtTabletTransactionLockConflict):
                example_env.client.commit_transaction(tx_id)


class TestAttributeSensors:
    def test(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)

        example_env.client.update_object("book", str(book), set_updates=[{"path": "/spec/page_count", "value": 1217}])

        leader_fqdn = get_leader_fqdn(example_env.yt_client, "//home/example/master", "Example master")
        profiler = Profiler(
            example_env.yt_client,
            "//home/example/master/instances/{}/orchid/sensors".format(leader_fqdn),
            fixed_tags={
                "object_type": "book",
            },
        )

        wait(lambda: profiler.get_all("objects/attribute_value") != [])


class TestTouchObjectTags:
    def test(self, example_env):
        book = example_env.create_book()
        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book",
            book,
            set_updates=[{"path": "/control/touch", "value": {}}],
        )
        timestamp = example_env.client.generate_timestamp()
        result = example_env.client.watch_objects(
            "book",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=30),
        )
        assert schema_pb2.ETag.TAG_BOOK_MODERATION in result[0]["changed_tags"]


class TestMultiEtc:
    def test_create_and_get(self, example_env):
        book = example_env.create_book(
            spec={
                "page_count": 356,
                "in_stock": 1000,
            }
        )
        result = example_env.client.get_object("book", str(book), ["/spec"])[0]
        assert result["page_count"] == 356
        assert result["in_stock"] == 1000

    def test_concurrent_update(self, example_env):
        book = example_env.create_book(
            spec={
                "description": "C++ Programming Language, Fourth Edition!",
                "in_stock": 1000,
            }
        )
        tx1 = example_env.client.start_transaction()
        tx2 = example_env.client.start_transaction()
        example_env.client.update_object(
            "book",
            str(book),
            set_updates=[
                {"path": "/spec/description", "value": "Stroustrup C++ Programming Language, Fourth Edition."}
            ],
            transaction_id=tx1,
        )
        example_env.client.update_object(
            "book", str(book), set_updates=[{"path": "/spec/in_stock", "value": 800}], transaction_id=tx2
        )
        example_env.client.commit_transaction(tx1)
        example_env.client.commit_transaction(tx2)
        result = example_env.client.get_object("book", str(book), ["/spec"])[0]
        assert result["description"] == "Stroustrup C++ Programming Language, Fourth Edition."
        assert result["in_stock"] == 800


class TestMetaKeys:
    def test_read_count(self, example_env):
        book_keys = []
        for i in range(100):
            publisher_key = example_env.client.create_object("publisher")
            book_keys.append(
                example_env.client.create_object(
                    "book",
                    {
                        "meta": {"parent_key": publisher_key},
                        "spec": {"year": 1900 + i},
                    },
                )
            )

        select_result = example_env.client.select_objects(
            "book",
            selectors=["/meta/key"],
            common_options=dict(fetch_performance_statistics=True),
            enable_structured_response=True,
        )
        perf_stats = select_result["performance_statistics"]["read_phase_count"]

        assert 1 == perf_stats

    def test_key(self, example_env):
        book_keys = []
        publisher_keys = []
        for i in range(100):
            publisher_key = example_env.client.create_object("publisher")
            publisher_keys.append(publisher_key)
            book_keys.append(
                example_env.client.create_object(
                    "book",
                    {
                        "meta": {"parent_key": publisher_key},
                        "spec": {"year": 1900 + i},
                    },
                )
            )

        select_result = example_env.client.select_objects(
            "book",
            selectors=["/meta/parent_key", "/meta/key"],
            enable_structured_response=True,
        )["results"]

        assert set([select_result[i][0]["value"] for i in range(100)]) == set(publisher_keys)
        assert set([select_result[i][1]["value"] for i in range(100)]) == set(book_keys)


class TestComputedFilter:
    EXAMPLE_MASTER_CONFIG = {
        "transaction_manager": {
            "build_key_expression": False,
        },
    }

    def test_in(self, example_env):
        book_keys = []
        filtered_keys = []
        publishers = []
        for i in range(100):
            publisher_key = example_env.client.create_object("publisher")
            book_keys.append(
                example_env.client.create_object(
                    "book",
                    {
                        "meta": {"parent_key": publisher_key},
                        "spec": {
                            "year": 1900 + i,
                        },
                    },
                )
            )
            if i % 10 == 0:
                publishers.append(publisher_key)
                if (10 <= i <= 30):
                    filtered_keys.append(book_keys[-1])

        filter_in = "('" + "', '".join(publishers) + "')"

        select_result = example_env.client.select_objects(
            "book",
            filter="[/spec/year] between 1910 and 1930 and [/meta/parent_key] in {}".format(filter_in),
            selectors=["/meta/key"],
            enable_structured_response=True,
        )
        results = select_result["results"]

        assert list(sorted([result[0]["value"] for result in results])) == list(sorted(filtered_keys))

    def test_pagination(self, example_env):
        book_keys = []
        filtered_keys = []
        publishers = []
        fonts = ["Garamond", "Arial", "Times New Roman", "Calibri", "Comic Sans MC"]
        for i in range(100):
            publisher_key = example_env.client.create_object("publisher")
            book_keys.append(
                example_env.client.create_object(
                    "book",
                    {
                        "meta": {"parent_key": publisher_key},
                        "spec": {
                            "year": 1900 + i,
                            "font": fonts[i % 5]
                        },
                    },
                )
            )
            if (30 <= i <= 75 and i % 4 == 0):
                publishers.append(publisher_key)
                filtered_keys.append(book_keys[-1])

        filter_in = "('" + "', '".join(publishers) + "')"
        limit = 3
        continuation_token = None
        selected_book_keys = []
        order_by = [{"expression": "[/spec/font]"}]

        while True:
            options = dict()
            if continuation_token is not None:
                options["continuation_token"] = continuation_token
            response = example_env.client.select_objects(
                "book",
                filter="[/spec/year] between 1930 and 1975 and [/meta/parent_key] in {}".format(filter_in),
                selectors=["/meta/key"],
                limit=3,
                order_by=order_by,
                options=options,
                index="books_by_year",
                enable_structured_response=True,
            )
            results = response["results"]
            continuation_token = response["continuation_token"]
            for result in results:
                selected_book_keys.append(result[0]["value"])
            if len(results) < limit:
                break

        assert list(sorted(filtered_keys)) == list(sorted(selected_book_keys))

    def test_composite(self, example_env):
        book_keys = []
        filtered_keys = []
        for i in range(100):
            publisher_key = example_env.client.create_object("publisher")
            book_keys.append(
                example_env.client.create_object(
                    "book",
                    {
                        "meta": {"parent_key": publisher_key},
                        "spec": {
                            "year": 1900 + i,
                        },
                    },
                )
            )
            if (10 <= i <= 30):
                filtered_keys.append(book_keys[-1])

        select_result = example_env.client.select_objects(
            "book",
            filter="any_to_yson_string([/meta]) not like '%!%' and [/spec/year] between 1910 and 1930",
            selectors=["/meta/key"],
            enable_structured_response=True,
        )
        results = select_result["results"]

        assert list(sorted([result[0]["value"] for result in results])) == list(sorted(filtered_keys))

    def test_nested(self, example_env):
        book_keys = []
        filtered_keys = []
        publishers = []
        colors = ["red", "orange", "yellow", "green", "blue", "indigo", "violet", "white", "black", "brown"]
        for i in range(100):
            publisher_key = example_env.client.create_object("publisher")
            book_keys.append(
                example_env.client.create_object(
                    "book",
                    {
                        "meta": {"parent_key": publisher_key},
                        "spec": {
                            "design": {
                                "color" : colors[i % 10]
                            }
                        },
                    },
                )
            )
            if (i % 10 in [0, 5, 8]):
                filtered_keys.append(book_keys[-1])
                if (i % 10 == 5):
                    publishers.append(publisher_key)

        filter_in = "('" + "', '".join(publishers) + "')"
        limit = 35

        select_result = example_env.client.select_objects(
            "book",
            filter="[/spec/design/color] in ('red', 'black') or [/meta/parent_key] in {}".format(filter_in),
            limit=limit,
            selectors=["/meta/key"],
            enable_structured_response=True,
        )
        results = select_result["results"]

        assert list(sorted([result[0]["value"] for result in results])) == list(sorted(filtered_keys))


class TestSynchronousRemoval:
    EXAMPLE_MASTER_CONFIG = {
        "object_manager": {
            "remove_mode": "synchronous",
        }
    }

    def test_remove_snapshot(self, example_env):
        book = example_env.create_book()
        example_env.client.remove_object("book", str(book))

        assert 0 == len(list(example_env.yt_client.select_rows("* from [//home/example/db/books]")))
