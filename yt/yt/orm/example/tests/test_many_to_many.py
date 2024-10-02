from .test_incomplete_base import IncompleteBaseTest
from .conftest import format_object_key

from yt.orm.library.common import NoSuchObjectError, RemovalForbiddenError

import pytest


class TestManyToMany:
    MANY_TO_MANY_TABLE = "//home/example/db/authors_to_books"

    @pytest.fixture(autouse=True)
    def clear_many_to_many_before_test(self, example_env):
        example_env.clear_dummy_content_table(self.MANY_TO_MANY_TABLE)

    @pytest.fixture
    def greg_id(self, example_env):
        return example_env.create_author(name="Greg")

    @pytest.fixture
    def anna_id(self, example_env):
        return example_env.create_author(name="Anna")

    @pytest.fixture
    def paul_id(self, example_env):
        return example_env.create_author(name="Paul")

    @pytest.fixture
    def publisher_id(self, example_env):
        return example_env.create_publisher(name="O'REILLY")

    @pytest.fixture
    def assert_author_ids(self, example_env, publisher_id):
        def _inner(book_id, author_ids):
            assert [
                {
                    "author_id": author_id,
                    "publisher_id": publisher_id,
                    "book_id": int(book_id.split(";")[0]),
                    "book_id2": int(book_id.split(";")[1]),
                    "dummy": None,
                }
                for author_id in sorted(author_ids)
            ] == sorted(
                example_env.get_table_rows(self.MANY_TO_MANY_TABLE),
                key=lambda row: row["author_id"],
            )
            assert [author_ids] == example_env.client.get_object(
                "book", str(book_id), ["/spec/author_ids"]
            )

        return _inner

    def test_create_several(self, greg_id, anna_id, example_env, assert_author_ids, publisher_id):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        assert_author_ids(book_id, [greg_id, anna_id])

    def test_create_single(self, greg_id, example_env, assert_author_ids, publisher_id):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id]))
        assert_author_ids(book_id, [greg_id])

    def test_create_empty_list(self, example_env, assert_author_ids, publisher_id):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[]))
        assert_author_ids(book_id, [])

    def test_create_unknown(self, example_env, assert_author_ids, publisher_id):
        with pytest.raises(NoSuchObjectError):
            example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=["unknown_author_id"]))

    def test_create_empty_value(self, example_env, publisher_id):
        with pytest.raises(NoSuchObjectError):
            example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[""]))

    def test_create_duplicate(self, greg_id, anna_id, example_env, assert_author_ids, publisher_id):
        book_id = example_env.create_book(
            publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id, greg_id])
        )
        assert_author_ids(book_id, [greg_id, anna_id])

    def test_update_add(
        self, greg_id, anna_id, example_env, assert_author_ids, publisher_id
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id]))
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": [greg_id, anna_id]}],
        )
        assert_author_ids(book_id, [greg_id, anna_id])

    def test_update_remove(
        self, greg_id, anna_id, example_env, assert_author_ids, publisher_id
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": [anna_id]}],
        )
        assert_author_ids(book_id, [anna_id])

    def test_update_partial(
        self,
        greg_id,
        anna_id,
        paul_id,
        example_env,
        assert_author_ids,
        publisher_id,
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": [anna_id, paul_id]}],
        )
        assert_author_ids(book_id, [anna_id, paul_id])

    def test_update_clear(
        self,
        greg_id,
        anna_id,
        paul_id,
        example_env,
        assert_author_ids,
        publisher_id,
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": []}],
        )
        assert_author_ids(book_id, [])

    def test_update_duplicate(
        self,
        greg_id,
        anna_id,
        paul_id,
        example_env,
        assert_author_ids,
        publisher_id,
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": [paul_id, anna_id, paul_id]}],
        )
        assert_author_ids(book_id, [paul_id, anna_id])

    def test_update_permutation(
        self,
        greg_id,
        anna_id,
        paul_id,
        example_env,
        assert_author_ids,
        publisher_id,
    ):
        book_id = example_env.create_book(
            publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id, paul_id])
        )
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": [paul_id, greg_id, anna_id]}],
        )
        assert_author_ids(book_id, [paul_id, greg_id, anna_id])

    def test_update_entity(
        self,
        greg_id,
        anna_id,
        example_env,
        publisher_id,
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/alternative_publisher_ids", "value": None}],
        )
        assert [[]] == example_env.client.get_object(
            "book", str(book_id), ["/spec/alternative_publisher_ids"]
        )

    def test_update_empty_list(
        self,
        greg_id,
        anna_id,
        example_env,
        publisher_id,
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/alternative_publisher_ids", "value": []}],
        )
        assert [[]] == example_env.client.get_object(
            "book", str(book_id), ["/spec/alternative_publisher_ids"]
        )

    def test_remove_tabular(self, greg_id, anna_id, example_env, assert_author_ids, publisher_id):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.remove_object("author", greg_id)
        assert_author_ids(book_id, [anna_id])
        example_env.client.remove_object("author", anna_id)
        assert_author_ids(book_id, [])

    def test_remove_inline(self, greg_id, anna_id, example_env, publisher_id):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))
        example_env.client.remove_object("book", format_object_key(book_id))
        assert [] == example_env.get_table_rows(self.MANY_TO_MANY_TABLE)

    def test_transaction_create_updates(self, example_env, greg_id, anna_id, assert_author_ids, publisher_id):
        transaction_id = example_env.client.start_transaction()
        book_id = example_env.create_book(
            publisher_id=publisher_id,
            spec=dict(author_ids=[greg_id]),
            transaction_id=transaction_id,
        )
        for author_ids in ([], [greg_id, anna_id], [anna_id, greg_id], [anna_id]):
            example_env.client.update_object(
                "book",
                format_object_key(book_id),
                set_updates=[{"path": "/spec/author_ids", "value": author_ids}],
                transaction_id=transaction_id,
            )
        example_env.client.commit_transaction(transaction_id)

        assert_author_ids(book_id, [anna_id])

    def test_transaction_create_update_remove(
        self, example_env, greg_id, anna_id, publisher_id
    ):
        transaction_id = example_env.client.start_transaction()
        book_id = example_env.create_book(
            publisher_id=publisher_id,
            spec=dict(author_ids=[greg_id]),
            transaction_id=transaction_id,
        )
        example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": [anna_id]}],
            transaction_id=transaction_id,
        )
        example_env.client.remove_object(
            "book", format_object_key(book_id), transaction_id=transaction_id
        )
        example_env.client.commit_transaction(transaction_id)

        assert [] == example_env.get_table_rows(self.MANY_TO_MANY_TABLE)

    def test_transaction_remove_create(
        self, example_env, greg_id, anna_id, assert_author_ids, publisher_id
    ):
        book_id = example_env.create_book(publisher_id=publisher_id, spec=dict(author_ids=[greg_id, anna_id]))

        transaction_id = example_env.client.start_transaction()
        example_env.client.remove_object(
            "book", format_object_key(book_id), transaction_id=transaction_id
        )
        example_env.create_book(
            publisher_id=publisher_id,
            spec=dict(author_ids=[anna_id, greg_id]),
            book_id=book_id,
            transaction_id=transaction_id,
        )
        example_env.client.commit_transaction(transaction_id)

        assert_author_ids(book_id, [anna_id, greg_id])

    def test_timestamps(self, example_env, greg_id, anna_id):
        def assert_timestamp(book_id, timestamp):
            get_response = example_env.client.get_object(
                "book",
                format_object_key(book_id),
                enable_structured_response=True,
                options={"fetch_timestamps": True, "fetch_values": False},
                selectors=["/spec/author_ids"],
            )
            assert get_response["timestamp"] > timestamp
            assert get_response["result"][0]["timestamp"] == timestamp

        create_response = example_env.client.create_object(
            "book",
            {
                "meta": {
                    "isbn": "978-1449355739",
                    "parent_key": example_env.client.create_object(
                        "publisher",
                        {"spec": {"name": "O'REILLY"}},
                        request_meta_response=True,
                    ),
                },
                "spec": {
                    "author_ids": [greg_id],
                },
            },
            enable_structured_response=True,
            request_meta_response=True,
        )
        book_id = create_response["meta"]["key"]
        assert_timestamp(book_id, create_response["commit_timestamp"])

        update_response = example_env.client.update_object(
            "book",
            format_object_key(book_id),
            set_updates=[{"path": "/spec/author_ids", "value": [anna_id]}],
        )
        assert_timestamp(book_id, update_response["commit_timestamp"])

    def test_read_phase_count(self, example_env):
        client = example_env.client

        publishers = example_env.client.create_objects(
            (
                ("publisher", dict(spec=dict(name="publisher_{}".format(publisher_index))))
                for publisher_index in range(1000)
            ),
        )

        book = client.create_object(
            "book",
            {
                "meta": {
                    "parent_key": publishers[0],
                },
                "spec": {
                    "alternative_publisher_ids": list(map(int, publishers))
                },
            },
        )

        result = client.get_object(
            "book",
            book,
            selectors=["/spec/publishers/*/meta/id"],
            common_options=dict(fetch_performance_statistics=True),
            enable_structured_response=True)["performance_statistics"]
        assert 6 == result["read_phase_count"]

    def test_forbid_non_empty_removal(self, example_env):
        pub1 = example_env.create_publisher(name="first")
        pub2 = example_env.create_publisher(name="second")
        books = []
        for _ in range(2):
            books.append(
                example_env.client.create_object(
                    "book",
                    {
                        "meta": {
                            "isbn": "978-1449355739",
                            "parent_key": example_env.client.create_object(
                                "publisher",
                                {"spec": {"name": "O'REILLY"}},
                                request_meta_response=True,
                            ),
                        },
                        "spec": {
                            "alternative_publisher_ids": [pub1, pub2],
                        },
                    },
                )
            )

        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object("book", books[0])
        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object("publisher", str(pub1))
        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object(
                "book", books[0], allow_removal_with_non_empty_reference=False
            )
        example_env.client.remove_object("book", books[0], allow_removal_with_non_empty_reference=True)

        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object("publisher", str(pub1))
        patch = dict(transaction_manager=dict(allow_removal_with_nonempty_reference_override=True))
        with example_env.set_cypress_config_patch_in_context(patch):
            example_env.client.remove_object("publisher", str(pub2))

        with pytest.raises(RemovalForbiddenError):
            example_env.client.remove_object("publisher", str(pub1))
        example_env.client.remove_object(
            "publisher", str(pub1), allow_removal_with_non_empty_reference=True
        )

    class TestOneToManyIncomplete(IncompleteBaseTest):
        EXAMPLE_MASTER_CONFIG = {
            "transaction_manager": {
                "any_to_one_attribute_value_getter_returns_null": False,
            },
        }

        @pytest.fixture
        def piter_publisher(self, example_env):
            return example_env.create_publisher(name="Piter")

        @pytest.fixture
        def orelly_publisher(self, example_env):
            return example_env.create_publisher(name="O'REILLY")

        @pytest.fixture
        def object_type(self):
            return "book"

        @pytest.fixture
        def old_value(self):
            return []

        @pytest.fixture
        def object_id(self, orelly_publisher, old_value, example_env):
            return example_env.create_book(
                publisher_id=orelly_publisher,
                spec=dict(alternative_publisher_ids=old_value),
            )

        @pytest.fixture
        def primary_key(self, object_id, orelly_publisher):
            parts = str(object_id).split(";")
            if len(parts) == 2:
                return {
                    "meta.id": int(parts[0]),
                    "meta.id2": int(parts[1]),
                    "meta.publisher_id": orelly_publisher,
                }
            return {"meta.id": object_id, "meta.publisher_id": orelly_publisher}

        @pytest.fixture
        def incomplete_path_items(self):
            return ["spec", "alternative_publisher_ids"]

        @pytest.fixture
        def new_value(self, piter_publisher):
            return [piter_publisher]
