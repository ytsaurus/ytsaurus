from yt.orm.library.common import YtResponseError

import pytest

# index               | 1st                                     | 2nd                                                                                    |
# indexed_attribute   | /spec/year (S), /spec/font (S)          | /spec/design/illustrations (R)                                                         |
# predicate_attribute | /spec/design/cover/hardcover (S)        | /spec/font (S), /spec/design/illustrations (R)                                         |
# predicate           | "[/spec/design/cover/hardcover] = %true"| [/spec/font] != \"arial\" or [list_contains([/spec/design/illustrations], \"cat.svg\") |

FIRST_INDEX_FALSE_PREDICATE_ARGS = {"hardcover": False}
FIRST_INDEX_TRUE_PREDICATE_ARGS = {"hardcover": True}
SECOND_INDEX_FALSE_PREDICATE_ARGS = {"font": "Arial", "illustrations": ["dog.png", "whale.jpeg"]}
SECOND_INDEX_TRUE_PREDICATE_ARGS = {"font": "Times New Roman", "illustrations": ["cat.svg"]}

DEFAULT_YEAR = 2023
DEFAULT_PAGE_COUNT = 123

BOOKS_BY_YEAR_AND_PAGE_COUNT_TABLE_WITH_PREDICATE = "//home/example/db/books_by_year_and_page_count_with_predicate"
BOOKS_BY_ILLUSTRATIONS_TABLE_WITH_PREDICATE = "//home/example/db/books_by_illustrations_with_predicate"


class TestIndexWithPredicate:

    @pytest.fixture
    def transaction_id(self, example_env):
        def _inner():
            return example_env.client.start_transaction()
        return _inner

    @pytest.fixture
    def commit_transaction(self, example_env):
        def _inner(transaction_id):
            example_env.client.commit_transaction(transaction_id)
        return _inner

    @pytest.fixture(autouse=True)
    def clear_index_tables_before_test(self, example_env):
        for index_table_name in (
                BOOKS_BY_YEAR_AND_PAGE_COUNT_TABLE_WITH_PREDICATE,
                BOOKS_BY_ILLUSTRATIONS_TABLE_WITH_PREDICATE,
        ):
            example_env.clear_dummy_content_table(index_table_name)

    @pytest.fixture
    def publisher_id(self, example_env):
        return int(example_env.create_object("publisher", spec={"name": "O'REILLY"}))

    @pytest.fixture
    def create_book(self, example_env, publisher_id):
        def _inner(
            *,
            hardcover,
            illustrations,
            font,
            year=DEFAULT_YEAR,
            page_count=DEFAULT_PAGE_COUNT,
            transaction_id=None
        ):
            return example_env.create_object(
                "book",
                meta={"publisher_id": publisher_id},
                spec={
                    "year": year,
                    "font": font,
                    "design": {
                        "cover": {
                            "hardcover": hardcover,
                        },
                        "illustrations": illustrations,
                    },
                    "page_count": page_count,
                },
                transaction_id=transaction_id
            )
        return _inner

    @pytest.fixture
    def update_book(self, example_env):
        def _inner(id, *, year=None, hardcover=None, illustrations=None, font=None, transaction_id=None):
            patch = [
                {"path": "/spec/year", "value": year},
                {"path": "/spec/design/cover/hardcover", "value": hardcover},
                {"path": "/spec/design/illustrations", "value": illustrations},
                {"path": "/spec/font", "value": font},
            ]
            return example_env.client.update_object(
                "book",
                str(id),
                set_updates=filter(lambda p: p["value"] is not None, patch),
                transaction_id=transaction_id,
            )
        return _inner

    @pytest.fixture
    def remove_book(self, example_env):
        def _inner(id, *, transaction_id=None):
            return example_env.client.remove_object("book", str(id), transaction_id=transaction_id)
        return _inner

    @pytest.fixture
    def touch_indexes(self, example_env):
        def _inner(id, *, transaction_id=None):
            return example_env.client.update_object(
                "book",
                str(id),
                set_updates=[
                    {
                        "path": "/control/touch_index",
                        "value": {"index_names": ["books_by_year_and_page_count_with_predicate"]}
                    },
                    {
                        "path": "/control/touch_index",
                        "value": {"index_names": ["books_by_illustrations_with_predicate"]}
                    },
                ],
                transaction_id=transaction_id
            )
        return _inner

    @pytest.fixture
    def assert_first_empty(self, example_env):
        def _inner(*unused_args, **unused_kwars):
            assert [] == example_env.get_table_rows(BOOKS_BY_YEAR_AND_PAGE_COUNT_TABLE_WITH_PREDICATE)
        return _inner

    @pytest.fixture
    def assert_second_empty(self, example_env):
        def _inner(*unused_args, **unused_kwars):
            assert [] == example_env.get_table_rows(BOOKS_BY_ILLUSTRATIONS_TABLE_WITH_PREDICATE)
        return _inner

    @pytest.fixture
    def assert_both_empty(self, assert_first_empty, assert_second_empty):
        def _inner(*unused_args, **unused_kwars):
            assert_first_empty()
            assert_second_empty()
        return _inner

    @pytest.fixture
    def assert_first_one(self, example_env, publisher_id):
        def _inner(id, *, year=None, page_count=None):
            id, id2 = map(int, id.split(";"))
            if year is None:
                year = DEFAULT_YEAR
            if page_count is None:
                page_count = DEFAULT_PAGE_COUNT
            assert [
                {
                    "year": year,
                    "page_count": page_count,
                    "publisher_id": publisher_id,
                    "book_id": id,
                    "book_id2": id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(BOOKS_BY_YEAR_AND_PAGE_COUNT_TABLE_WITH_PREDICATE)
        return _inner

    @pytest.fixture
    def assert_second_one(self, example_env, publisher_id):
        def _inner(id, *, illustrations):
            id, id2 = map(int, id.split(";"))
            assert [
                {
                    "design_illustrations": illustration,
                    "publisher_id": publisher_id,
                    "book_id": id,
                    "book_id2": id2,
                    "dummy": None,
                }
                for illustration in sorted(illustrations)
            ] == sorted(
                example_env.get_table_rows(BOOKS_BY_ILLUSTRATIONS_TABLE_WITH_PREDICATE),
                key=lambda row: row["design_illustrations"],
            )
        return _inner

    def test_empty(self, assert_both_empty):
        assert_both_empty()

    def test_create_and_remove(
        self,
        create_book,
        remove_book,
        assert_both_empty,
        assert_first_one,
        assert_second_one,
    ):
        id = create_book(**FIRST_INDEX_FALSE_PREDICATE_ARGS, **SECOND_INDEX_FALSE_PREDICATE_ARGS)
        assert_both_empty()
        remove_book(id)
        assert_both_empty()

        id = create_book(**FIRST_INDEX_TRUE_PREDICATE_ARGS, **SECOND_INDEX_TRUE_PREDICATE_ARGS)
        assert_first_one(id)
        assert_second_one(id, illustrations=SECOND_INDEX_TRUE_PREDICATE_ARGS["illustrations"])
        remove_book(id)
        assert_both_empty()

    def test_create_many(
        self,
        publisher_id,
        example_env,
    ):
        book_keys = example_env.client.create_objects(
            (
                (
                    "book",
                    dict(
                        meta=dict(publisher_id=publisher_id),
                        spec=dict(
                            year=DEFAULT_YEAR,
                            font=second_index_args["font"],
                            design=dict(
                                cover=dict(hardcover=first_index_args["hardcover"]),
                                illustrations=second_index_args["illustrations"]
                            ),
                            page_count=DEFAULT_PAGE_COUNT,
                        )
                    )
                )
                for first_index_args in (FIRST_INDEX_FALSE_PREDICATE_ARGS, FIRST_INDEX_TRUE_PREDICATE_ARGS)
                for second_index_args in (SECOND_INDEX_FALSE_PREDICATE_ARGS, SECOND_INDEX_TRUE_PREDICATE_ARGS)
                for _ in range(10)
            ),
        )

        ids_with_true_second_predicate = book_keys[10:20] + book_keys[30:40]
        ids_with_true_first_predicate = book_keys[20:40]

        def split_id(ids):
            return [[int(id_part) for id_part in id.split(";")] for id in ids]

        ids_with_true_first_predicate = split_id(ids_with_true_first_predicate)
        ids_with_true_second_predicate = split_id(ids_with_true_second_predicate)

        assert [
            {
                "year": DEFAULT_YEAR,
                "page_count": DEFAULT_PAGE_COUNT,
                "publisher_id": publisher_id,
                "book_id": id[0],
                "book_id2": id[1],
                "dummy": None,
            }
            for id in sorted(ids_with_true_first_predicate)
        ] == sorted(
            example_env.get_table_rows(BOOKS_BY_YEAR_AND_PAGE_COUNT_TABLE_WITH_PREDICATE),
            key=lambda row: (row["book_id"], row["book_id2"])
        )
        assert [
            {
                "design_illustrations": illustration,
                "publisher_id": publisher_id,
                "book_id": id[0],
                "book_id2": id[1],
                "dummy": None,
            }
            for illustration in sorted(SECOND_INDEX_TRUE_PREDICATE_ARGS["illustrations"])
            for id in sorted(ids_with_true_second_predicate)
        ] == sorted(
            example_env.get_table_rows(BOOKS_BY_ILLUSTRATIONS_TABLE_WITH_PREDICATE),
            key=lambda row: (row["design_illustrations"], row["book_id"], row["book_id2"])
        )

    @pytest.mark.parametrize(
        "first_predicate_args_init",
        [
            pytest.param(FIRST_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(FIRST_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "hardcover, check_first",
        [
            pytest.param(False, "assert_first_empty", id='?->F'),
            pytest.param(True, "assert_first_one", id='?->T'),
        ]
    )
    @pytest.mark.parametrize(
        "second_predicate_args_init, font, illustrations, check_second",
        [
            pytest.param(
                SECOND_INDEX_FALSE_PREDICATE_ARGS,
                None,
                ["dog.png", "whale.jpeg"],
                "assert_second_empty",
                id="->F->F (not indexed F, indexed ->F)",
            ),
            pytest.param(
                SECOND_INDEX_FALSE_PREDICATE_ARGS,
                "Arial",
                None,
                "assert_second_empty",
                id="->F->F (not indexed ->F, indexed F)"
            ),
            pytest.param(
                SECOND_INDEX_FALSE_PREDICATE_ARGS,
                "Arial",
                ["dog.png", "whale.jpeg"],
                "assert_second_empty",
                id="->F->F (not indexed ->F, indexed ->F)"
            ),
            pytest.param(
                SECOND_INDEX_FALSE_PREDICATE_ARGS,
                None,
                ["cat.svg"],
                "assert_second_one",
                id="->F->T (not indexed F, indexed ->T)"
            ),
            pytest.param(
                SECOND_INDEX_FALSE_PREDICATE_ARGS,
                "Times New Roman",
                None,
                "assert_second_one",
                id="->F->T (not indexed ->T, indexed F)"
            ),
            pytest.param(
                SECOND_INDEX_FALSE_PREDICATE_ARGS,
                "Times New Roman",
                ["cat.svg"],
                "assert_second_one",
                id="->F->T (not indexed ->T, indexed ->T)"
            ),
            pytest.param(
                SECOND_INDEX_TRUE_PREDICATE_ARGS,
                None,
                ["dog.png", "whale.jpeg"],
                "assert_second_one",
                id="->T->T (not indexed T, indexed ->F)"
            ),
            pytest.param(
                SECOND_INDEX_TRUE_PREDICATE_ARGS,
                "Arial",
                None,
                "assert_second_one",
                id="->T->T (not indexed ->T, indexed F)"
            ),
            pytest.param(
                SECOND_INDEX_TRUE_PREDICATE_ARGS,
                "Arial",
                ["dog.png", "whale.jpeg"],
                "assert_second_empty",
                id="->T->T (not indexed ->F, indexed ->F)"
            ),
            pytest.param(
                SECOND_INDEX_TRUE_PREDICATE_ARGS,
                None,
                ["cat.svg"],
                "assert_second_one",
                id="->T->T (not indexed T, indexed ->T)"
            ),
            pytest.param(
                SECOND_INDEX_TRUE_PREDICATE_ARGS,
                "Times New Roman",
                None,
                "assert_second_one",
                id="->T->T (not indexed ->T, indexed T)"
            ),
            pytest.param(
                SECOND_INDEX_TRUE_PREDICATE_ARGS,
                "Times New Roman",
                ["cat.svg"],
                "assert_second_one",
                id="->T->T (not indexed ->T, indexed ->T)"
            ),
        ]
    )
    def test_update(
        self,
        request,
        create_book,
        first_predicate_args_init,
        second_predicate_args_init,
        update_book,
        hardcover,
        font,
        illustrations,
        check_first,
        check_second
    ):
        check_first = request.getfixturevalue(check_first)
        check_second = request.getfixturevalue(check_second)

        id = create_book(**first_predicate_args_init, **second_predicate_args_init)
        update_book(id, hardcover=hardcover, font=font, illustrations=illustrations)
        check_first(id)
        check_second(
            id,
            illustrations=illustrations if illustrations is not None else second_predicate_args_init["illustrations"]
        )

    @pytest.mark.parametrize(
        "first_predicate_args_init, check_first",
        [
            pytest.param(FIRST_INDEX_FALSE_PREDICATE_ARGS, "assert_first_empty", id='->F'),
            pytest.param(FIRST_INDEX_TRUE_PREDICATE_ARGS, "assert_first_one", id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "second_predicate_args_init, check_second",
        [
            pytest.param(SECOND_INDEX_FALSE_PREDICATE_ARGS, "assert_second_empty", id='->F'),
            pytest.param(SECOND_INDEX_TRUE_PREDICATE_ARGS, "assert_second_one", id='->T'),
        ]
    )
    def test_create_and_touch(
        self,
        create_book,
        first_predicate_args_init,
        second_predicate_args_init,
        touch_indexes,
        request,
        check_first,
        check_second,
    ):
        check_first = request.getfixturevalue(check_first)
        check_second = request.getfixturevalue(check_second)

        id = create_book(**first_predicate_args_init, **second_predicate_args_init)
        check_first(id)
        check_second(id, illustrations=second_predicate_args_init["illustrations"])

        touch_indexes(id)

        check_first(id)
        check_second(id, illustrations=second_predicate_args_init["illustrations"])

    def test_touch_after_disabling(
        self,
        example_env,
        create_book,
        update_book,
        touch_indexes,
        assert_first_one,
        assert_first_empty,
    ):
        id = create_book(**FIRST_INDEX_TRUE_PREDICATE_ARGS, **SECOND_INDEX_FALSE_PREDICATE_ARGS)
        assert_first_one(id)

        config = {
            "object_manager": {"index_mode_per_name": {"books_by_year_and_page_count_with_predicate": "disabled"}}
        }
        with example_env.set_cypress_config_patch_in_context(config):
            update_book(id, hardcover=FIRST_INDEX_FALSE_PREDICATE_ARGS["hardcover"])
            assert_first_one(id)

        touch_indexes(id)
        assert_first_empty()

    @pytest.mark.parametrize(
        "first_predicate_args_init",
        [
            pytest.param(FIRST_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(FIRST_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "hardcover, check_first",
        [
            pytest.param(False, "assert_first_empty", id='?->F'),
            pytest.param(True, "assert_first_one", id='?->T'),
        ]
    )
    @pytest.mark.parametrize(
        "second_predicate_args_init",
        [
            pytest.param(SECOND_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(SECOND_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "font, illustrations, check_second",
        [
            pytest.param("Arial", ["dog.png", "whale.jpeg"], "assert_second_empty", id="?->F"),
            pytest.param("Times New Roman", ["cat.svg"], "assert_second_one", id="?->T"),
        ]
    )
    def test_create_and_update_in_transaction(
        self,
        create_book,
        first_predicate_args_init,
        second_predicate_args_init,
        update_book,
        hardcover,
        font,
        illustrations,
        transaction_id,
        commit_transaction,
        request,
        check_first,
        check_second
    ):
        check_first = request.getfixturevalue(check_first)
        check_second = request.getfixturevalue(check_second)

        transaction_id = transaction_id()
        id = create_book(**first_predicate_args_init, **second_predicate_args_init, transaction_id=transaction_id)
        update_book(id, hardcover=hardcover, font=font, illustrations=illustrations, transaction_id=transaction_id)
        commit_transaction(transaction_id)

        check_first(id)
        check_second(id, illustrations=illustrations)

    @pytest.mark.parametrize(
        "first_predicate_args_init",
        [
            pytest.param(FIRST_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(FIRST_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize("hardcover", [pytest.param(False, id='?->F'), pytest.param(True, id='?->T')])
    @pytest.mark.parametrize(
        "second_predicate_args_init",
        [
            pytest.param(SECOND_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(SECOND_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "font, illustrations",
        [
            pytest.param("Arial", ["dog.png", "whale.jpeg"], id="?->F"),
            pytest.param("Times New Roman", ["cat.svg"], id="?->T"),
        ]
    )
    def test_update_and_remove_in_transaction(
        self,
        create_book,
        first_predicate_args_init,
        second_predicate_args_init,
        update_book,
        hardcover,
        font,
        illustrations,
        transaction_id,
        commit_transaction,
        remove_book,
        assert_both_empty,
    ):
        id = create_book(**first_predicate_args_init, **second_predicate_args_init)

        transaction_id = transaction_id()
        update_book(id, hardcover=hardcover, font=font, illustrations=illustrations, transaction_id=transaction_id)
        remove_book(id, transaction_id=transaction_id)
        commit_transaction(transaction_id)

        assert_both_empty()

    @pytest.mark.parametrize(
        "first_predicate_args_init",
        [
            pytest.param(FIRST_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(FIRST_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "second_predicate_args_init",
        [
            pytest.param(SECOND_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(SECOND_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    def test_create_and_remove_in_transaction(
        self,
        create_book,
        first_predicate_args_init,
        second_predicate_args_init,
        transaction_id,
        commit_transaction,
        remove_book,
        assert_both_empty,
    ):
        transaction_id = transaction_id()
        id = create_book(**first_predicate_args_init, **second_predicate_args_init, transaction_id=transaction_id)
        remove_book(id, transaction_id=transaction_id)
        commit_transaction(transaction_id)

        assert_both_empty()

    @pytest.mark.parametrize(
        "first_predicate_args_init",
        [
            pytest.param(FIRST_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(FIRST_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize("hardcover", [pytest.param(False, id='?->F'), pytest.param(True, id='?->T')])
    @pytest.mark.parametrize(
        "second_predicate_args_init",
        [
            pytest.param(SECOND_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(SECOND_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "font, illustrations",
        [
            pytest.param("Arial", ["dog.png", "whale.jpeg"], id="?->F"),
            pytest.param("Times New Roman", ["cat.svg"], id="?->T"),
        ]
    )
    def test_create_update_and_remove_in_transaction(
        self,
        create_book,
        first_predicate_args_init,
        second_predicate_args_init,
        update_book,
        hardcover,
        font,
        illustrations,
        remove_book,
        transaction_id,
        commit_transaction,
        assert_both_empty
    ):
        transaction_id = transaction_id()
        id = create_book(**first_predicate_args_init, **second_predicate_args_init, transaction_id=transaction_id)
        update_book(id, hardcover=hardcover, font=font, illustrations=illustrations, transaction_id=transaction_id)
        remove_book(id, transaction_id=transaction_id)
        commit_transaction(transaction_id)

        assert_both_empty()

    @pytest.mark.parametrize(
        "first_predicate_args_init",
        [
            pytest.param(FIRST_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(FIRST_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    @pytest.mark.parametrize(
        "second_predicate_args_init",
        [
            pytest.param(SECOND_INDEX_FALSE_PREDICATE_ARGS, id='->F'),
            pytest.param(SECOND_INDEX_TRUE_PREDICATE_ARGS, id='->T'),
        ]
    )
    def test_many_updates_in_transaction(
        self,
        create_book,
        first_predicate_args_init,
        second_predicate_args_init,
        update_book,
        transaction_id,
        commit_transaction,
        assert_first_one,
        assert_second_empty
    ):
        id = create_book(**first_predicate_args_init, **second_predicate_args_init)

        transaction_id = transaction_id()
        # first predicate: ?->T->F->T
        # second predicate: ?-F->T->F
        for hardcover, font, illustrations in zip(
            (True, False, True),
            ("Arial", "Times New Roman", "Arial"),
            (["dog.png", "whale.jpeg"], ["cat.svg"], ["dog.png", "whale.jpeg"])
        ):
            update_book(id, hardcover=hardcover, font=font, illustrations=illustrations, transaction_id=transaction_id)
        commit_transaction(transaction_id)

        assert_first_one(id)
        assert_second_empty(id, illustrations=["dog.png", "whale.jpeg"])

    def test_wrong_attribute_type_in_predicate(self, example_env, publisher_id):
        config = {
            "object_manager": {"index_mode_per_name": {"books_by_isbn_with_predicate": "enabled"}}
        }
        with example_env.set_cypress_config_patch_in_context(config):
            with pytest.raises(YtResponseError, match="Cannot compare values of types \"string\" and \"int64\""):
                example_env.create_object("book", meta={"isbn": "978-1449355739", "publisher_id": publisher_id})
