from yt.orm.library.common import NoSuchObjectError, UniqueValueAlreadyExists
from yt.wrapper.errors import YtTabletTransactionLockConflict

import pytest
import time


class Any:
    def __eq__(self, other):
        return True


class TestIndexEngine:
    RESET_DB_BEFORE_TEST = False

    BOOKS_BY_YEAR_TABLE = "//home/example/db/books_by_year"
    BOOKS_BY_FONT_TABLE = "//home/example/db/books_by_font"
    BOOKS_BY_YEAR_AND_FONT_TABLE = "//home/example/db/books_by_year_and_font"
    EDITOR_TO_BOOKS_TABLE = "//home/example/db/editor_to_books"
    BOOKS_BY_GENRES_TABLE = "//home/example/db/books_by_genres"
    BOOKS_BY_KEYWORDS_TABLE = "//home/example/db/books_by_keywords"
    BOOKS_BY_ILLUSTRATIONS_TABLE = "//home/example/db/books_by_illustrations"
    BOOKS_BY_COVER_IMAGE_TABLE = "//home/example/db/books_by_cover_image"
    BOOKS_BY_PAGE_COUNT_TABLE = "//home/example/db/books_by_page_count"
    BOOKS_BY_NAME_TABLE = "//home/example/db/books_by_name"
    BOOKS_BY_STORE_RATING_TABLE = "//home/example/db/books_by_store_rating"
    AUTHORS_TO_BOOKS_TABLE = "//home/example/db/authors_to_books"
    BOOKS_BY_ISBN_TABLE = "//home/example/db/books_by_isbn"
    BOOKS_BY_CREATION_TIME_TABLE = "//home/example/db/books_by_creation_time"
    GENRE_BY_NAME_TABLE = "//home/example/db/genre_by_name"
    BOOKS_BY_COVER_SIZE_TABLE = "//home/example/db/books_by_cover_size"
    BOOKS_BY_EDITOR_AND_YEAR_TABLE_WITH_PREDICATE = "//home/example/db/books_by_editor_and_year_with_predicate"

    ################################################################################

    @pytest.fixture(scope="class", autouse=True)
    def create_other_editor(self, test_environment):
        test_environment.create_object("editor", meta={"id": "other_editor_id"}, spec={"name": "Alan"})

    @pytest.fixture(autouse=True)
    def clear_index_tables_before_test(self, example_env):
        for index_table_name in (
            self.BOOKS_BY_YEAR_TABLE,
            self.BOOKS_BY_FONT_TABLE,
            self.BOOKS_BY_YEAR_AND_FONT_TABLE,
            self.EDITOR_TO_BOOKS_TABLE,
            self.BOOKS_BY_GENRES_TABLE,
            self.BOOKS_BY_KEYWORDS_TABLE,
            self.BOOKS_BY_ILLUSTRATIONS_TABLE,
            self.BOOKS_BY_COVER_IMAGE_TABLE,
            self.BOOKS_BY_PAGE_COUNT_TABLE,
            self.BOOKS_BY_NAME_TABLE,
            self.AUTHORS_TO_BOOKS_TABLE,
            self.BOOKS_BY_STORE_RATING_TABLE,
            self.BOOKS_BY_ISBN_TABLE,
            self.BOOKS_BY_CREATION_TIME_TABLE,
            self.BOOKS_BY_COVER_SIZE_TABLE,
            self.BOOKS_BY_EDITOR_AND_YEAR_TABLE_WITH_PREDICATE,
        ):
            example_env.clear_dummy_content_table(index_table_name)
        example_env.clear_table(self.GENRE_BY_NAME_TABLE, lambda key: key == "name")

    ################################################################################

    @pytest.fixture
    def transaction_id(self, example_env):
        return example_env.client.start_transaction()

    @pytest.fixture
    def publisher_id(self, example_env, transaction_id):
        return int(example_env.create_object("publisher", spec={"name": "O'REILLY"}, transaction_id=transaction_id))

    @pytest.fixture
    def editor_id(self, example_env, transaction_id):
        return example_env.create_object("editor", spec={"name": "Greg"}, transaction_id=transaction_id)

    @pytest.fixture
    def paul_author_id(self, example_env, transaction_id):
        return example_env.create_object("author", spec={"name": "Paul"}, transaction_id=transaction_id)

    @pytest.fixture
    def anna_author_id(self, example_env, transaction_id):
        return example_env.create_object("author", spec={"name": "Anna"}, transaction_id=transaction_id)

    @pytest.fixture
    def create_book(self, example_env, publisher_id, editor_id, paul_author_id, transaction_id):
        def _inner(
            year=2013,
            font="Arial",
            genres=["education", "programming"],
            keywords=["learning", "python"],
            illustrations=["img1", "img2"],
            page_count=256,
            author_ids=[paul_author_id],
        ):
            return example_env.create_object(
                "book",
                meta={
                    "isbn": "978-1449355739",
                    "publisher_id": int(publisher_id),
                },
                spec={
                    "year": year,
                    "font": font,
                    "genres": genres,
                    "keywords": keywords,
                    "design": {
                        "cover": {
                            "hardcover": True,
                            "image": "path/to/cover.png",
                            "size": 1024,
                            "dpi": 600,
                        },
                        "color": "white",
                        "illustrations": illustrations,
                    },
                    "editor_id": editor_id,
                    "page_count": page_count,
                    "digital_data": {
                        "store_rating": 4.9,
                    },
                    "author_ids": author_ids,
                    "description": "About the programming language",
                },
                transaction_id=transaction_id,
            )

        return _inner

    @pytest.fixture
    def update_book(self, example_env, transaction_id):
        def _inner(
            book_id,
            year=None,
            font=None,
            name=None,
            editor=None,
            genres=None,
            keywords=None,
            illustrations=None,
            page_count=None,
            author_ids=None,
            description=None,
            cover_size=None,
        ):
            patch = [
                {"path": "/spec/year", "value": year},
                {"path": "/spec/font", "value": font},
                {"path": "/spec/name", "value": name},
                {"path": "/spec/editor_id", "value": editor},
                {"path": "/spec/genres", "value": genres},
                {"path": "/spec/keywords", "value": keywords},
                {"path": "/spec/design/illustrations", "value": illustrations},
                {"path": "/spec/page_count", "value": page_count},
                {"path": "/spec/author_ids", "value": author_ids},
                {"path": "/spec/description", "value": description},
                {"path": "/spec/design/cover/size", "value": cover_size},
            ]
            return example_env.client.update_object(
                "book",
                str(book_id),
                set_updates=filter(lambda p: p["value"] is not None, patch),
                transaction_id=transaction_id,
            )

        return _inner

    @pytest.fixture
    def remove_book(self, example_env, transaction_id):
        def _inner(book_id):
            return example_env.client.remove_object("book", str(book_id), transaction_id)

        return _inner

    @pytest.fixture
    def commit_transaction(self, example_env, transaction_id):
        committed = False

        def _inner():
            nonlocal committed
            if not committed:
                example_env.client.commit_transaction(transaction_id)
                committed = True

        return _inner

    @pytest.fixture
    def book_id(self, create_book):
        return create_book()

    @pytest.fixture
    def assert_empty(self, example_env):
        def _inner():
            assert [] == example_env.get_table_rows(self.BOOKS_BY_YEAR_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_FONT_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_YEAR_AND_FONT_TABLE)
            assert [] == example_env.get_table_rows(self.EDITOR_TO_BOOKS_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_GENRES_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_KEYWORDS_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_ILLUSTRATIONS_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_COVER_IMAGE_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_PAGE_COUNT_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_NAME_TABLE)
            assert [] == example_env.get_table_rows(self.AUTHORS_TO_BOOKS_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_ISBN_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_STORE_RATING_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_CREATION_TIME_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_COVER_SIZE_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_EDITOR_AND_YEAR_TABLE_WITH_PREDICATE)

        return _inner

    @pytest.fixture
    def assert_one(self, example_env, publisher_id, editor_id, paul_author_id):
        def _inner(
            book_id,
            year=None,
            font=None,
            editor=None,
            genres=None,
            keywords=None,
            illustrations=None,
            cover_image=None,
            page_count=None,
            publisher=None,
            store_rating=None,
            author_ids=None,
            cover_size=None,
        ):
            book_id2 = int(book_id.split(';')[1])
            book_id = int(book_id.split(';')[0])
            year = year if year is not None else 2013
            font = font if font is not None else "Arial"
            editor = editor if editor is not None else editor_id
            genres = genres if genres is not None else ["education", "programming"]
            keywords = keywords if keywords is not None else ["learning", "python"]
            illustrations = illustrations if illustrations is not None else ["img1", "img2"]
            cover_image = cover_image if cover_image is not None else "path/to/cover.png"
            page_count = page_count if page_count is not None else 256
            publisher = publisher if publisher is not None else publisher_id
            store_rating = store_rating if store_rating is not None else 4.9
            author_ids = author_ids if author_ids is not None else [paul_author_id]
            cover_size = cover_size if cover_size is not None else 1024
            assert [
                {
                    "hash": Any(),
                    "year": year,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_YEAR_TABLE)
            assert [
                {
                    "font": font,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_FONT_TABLE)
            assert [
                {
                    "year": year,
                    "font": font,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_YEAR_AND_FONT_TABLE)
            assert [
                {
                    "hash": Any(),
                    "editor_id": editor_id,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
                for editor_id in [editor]
                if editor_id  # TODO: remove after YTORM-259 fixed
            ] == example_env.get_table_rows(self.EDITOR_TO_BOOKS_TABLE)
            assert [
                {
                    "genres": genre,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
                for genre in sorted(set(genres))
            ] == sorted(example_env.get_table_rows(self.BOOKS_BY_GENRES_TABLE), key=lambda row: row["genres"])
            assert [
                {
                    "hash": Any(),
                    "keywords": keyword,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
                for keyword in sorted(set(keywords))
            ] == sorted(
                example_env.get_table_rows(self.BOOKS_BY_KEYWORDS_TABLE), key=lambda row: row["keywords"]
            )
            assert [
                {
                    "design_illustrations": illustration,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
                for illustration in sorted(set(illustrations))
            ] == sorted(
                example_env.get_table_rows(self.BOOKS_BY_ILLUSTRATIONS_TABLE),
                key=lambda row: row["design_illustrations"],
            )
            assert [
                {
                    "design_cover_image": cover_image,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_COVER_IMAGE_TABLE)
            assert [
                {
                    "page_count": page_count,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_PAGE_COUNT_TABLE)
            assert [] == example_env.get_table_rows(self.BOOKS_BY_NAME_TABLE)
            assert [
                {
                    "store_rating": store_rating,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_STORE_RATING_TABLE)
            assert [
                {
                    "author_id": author_id,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
                for author_id in sorted(set(author_ids))
            ] == sorted(
                example_env.get_table_rows(self.AUTHORS_TO_BOOKS_TABLE), key=lambda row: row["author_id"]
            )
            assert [
                {
                    "hash": Any(),
                    "isbn": "978-1449355739",
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_ISBN_TABLE)
            assert [
                {
                    "creation_time": Any(),
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_CREATION_TIME_TABLE)
            assert [
                {
                    "design_cover_size": cover_size,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_COVER_SIZE_TABLE)
            assert [
                {
                    "editor_id": editor,
                    "year": year,
                    "publisher_id": publisher,
                    "book_id": book_id,
                    "book_id2": book_id2,
                    "dummy": None,
                }
            ] == example_env.get_table_rows(self.BOOKS_BY_EDITOR_AND_YEAR_TABLE_WITH_PREDICATE)

        return _inner

    ################################################################################

    def test_empty(self, assert_empty):
        assert_empty()

    @pytest.mark.parametrize(
        "genres",
        [
            pytest.param(["education", "programming"], id="simple genres"),
            pytest.param(["education", "education", "education"], id="duplicate genre"),
            pytest.param([], id="empty genres"),
        ],
    )
    def test_create_one(self, commit_transaction, assert_one, genres, create_book):
        book_id = create_book(genres=genres)
        commit_transaction()
        assert_one(book_id, genres=genres)

    def test_create_empty(self, example_env, assert_one):
        publisher_id = int(example_env.create_object("publisher"))
        book_id = example_env.create_object(
            "book",
            meta={
                "isbn": "978-1449355739",
                "publisher_id": int(publisher_id),
            },
        )
        assert_one(
            book_id,
            year=0,
            font="",
            editor="",
            genres=[],
            keywords=[],
            illustrations=[],
            cover_image="",
            page_count=0,
            publisher=publisher_id,
            store_rating=0.0,
            author_ids=[],
            cover_size=0,
        )

    def test_create_many(
        self,
        example_env,
        create_book,
        publisher_id,
        editor_id,
        paul_author_id,
        commit_transaction,
    ):

        years = range(2010, 2015)
        book_ids = [[int(p) for p in create_book(year=year).split(';')] for year in years]
        commit_transaction()
        assert [
            {
                "hash": Any(),
                "year": year,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for year, book_id in sorted(zip(years, book_ids))
        ] == sorted(
            example_env.get_table_rows(self.BOOKS_BY_YEAR_TABLE), key=lambda row: (row["year"], row["book_id"])
        )
        assert [
            {
                "font": "Arial",
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(example_env.get_table_rows(self.BOOKS_BY_FONT_TABLE), key=lambda row: int(row["book_id"]))
        assert [
            {
                "year": year,
                "font": "Arial",
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for year, book_id in sorted(zip(years, book_ids))
        ] == sorted(
            example_env.get_table_rows(self.BOOKS_BY_YEAR_AND_FONT_TABLE),
            key=lambda row: (row["year"], row["book_id"]),
        )
        assert [
            {
                "hash": Any(),
                "editor_id": editor_id,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(example_env.get_table_rows(self.EDITOR_TO_BOOKS_TABLE), key=lambda row: row["book_id"])
        assert [
            {
                "genres": genre,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for genre in ["education", "programming"]
            for book_id in sorted(book_ids)
        ] == sorted(
            example_env.get_table_rows(self.BOOKS_BY_GENRES_TABLE),
            key=lambda row: (row["genres"], row["book_id"]),
        )
        assert [
            {
                "hash": Any(),
                "keywords": keyword,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for keyword in ["learning", "python"]
            for book_id in sorted(book_ids)
        ] == sorted(
            example_env.get_table_rows(self.BOOKS_BY_KEYWORDS_TABLE),
            key=lambda row: (row["keywords"], row["book_id"]),
        )
        assert [
            {
                "design_illustrations": illustration,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for illustration in ["img1", "img2"]
            for book_id in sorted(book_ids)
        ] == sorted(
            example_env.get_table_rows(self.BOOKS_BY_ILLUSTRATIONS_TABLE),
            key=lambda row: (row["design_illustrations"], row["book_id"]),
        )
        assert [
            {
                "page_count": 256,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(example_env.get_table_rows(self.BOOKS_BY_PAGE_COUNT_TABLE), key=lambda row: row["book_id"])
        assert [] == example_env.get_table_rows(self.BOOKS_BY_NAME_TABLE)
        assert [
            {
                "store_rating": 4.9,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(
            example_env.get_table_rows(self.BOOKS_BY_STORE_RATING_TABLE), key=lambda row: row["book_id"]
        )
        assert [
            {
                "author_id": paul_author_id,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(example_env.get_table_rows(self.AUTHORS_TO_BOOKS_TABLE), key=lambda row: row["book_id"])
        assert [
            {
                "hash": Any(),
                "isbn": "978-1449355739",
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(example_env.get_table_rows(self.BOOKS_BY_ISBN_TABLE), key=lambda row: row["book_id"])
        assert [
            {
                "creation_time": Any(),
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(
            example_env.get_table_rows(self.BOOKS_BY_CREATION_TIME_TABLE),
            key=lambda row: row["book_id"],
        )
        assert [
            {
                "design_cover_size": 1024,
                "publisher_id": publisher_id,
                "book_id": book_id[0],
                "book_id2": book_id[1],
                "dummy": None,
            }
            for book_id in sorted(book_ids)
        ] == sorted(example_env.get_table_rows(self.BOOKS_BY_COVER_SIZE_TABLE), key=lambda row: row["book_id"])

    def test_update_non_index_field(self, update_book, book_id, commit_transaction, assert_one):
        update_book(book_id, description="updated description")
        commit_transaction()
        assert_one(book_id)

    @pytest.mark.parametrize(
        "year,font,editor,genres,keywords,illustrations,page_count,cover_size",
        [
            pytest.param(2014, None, None, None, None, None, None, None, id="year only"),
            pytest.param(
                None, "Times New Roman", None, None, None, None, None, None, id="font only"
            ),
            pytest.param(
                None, None, "other_editor_id", None, None, None, None, None, id="editor only"
            ),
            pytest.param(
                2015, "Comic Sans", None, None, None, None, None, None, id="year and font"
            ),
            pytest.param(
                None,
                None,
                None,
                ["education", "gold_collection"],
                None,
                None,
                None,
                None,
                id="genres only",
            ),
            pytest.param(
                None,
                None,
                None,
                None,
                ["learning", "o'relly"],
                None,
                None,
                None,
                id="keywords only",
            ),
            pytest.param(
                None, None, None, None, None, ["img2", "img3"], None, None, id="illustrations only"
            ),
            pytest.param(
                None,
                None,
                None,
                ["education", "programming"],
                None,
                None,
                None,
                None,
                id="equal genres",
            ),
            pytest.param(
                None,
                None,
                None,
                ["education", "programming", "education", "education"],
                None,
                None,
                None,
                None,
                id="duplicate genre",
            ),
            pytest.param(
                None,
                None,
                None,
                ["programming", "education"],
                None,
                None,
                None,
                None,
                id="genres permutation",
            ),
            pytest.param(
                None,
                None,
                None,
                ["education", "programming", "python"],
                None,
                None,
                None,
                None,
                id="add to genres",
            ),
            pytest.param(
                None, None, None, ["education"], None, None, None, None, id="remove from genres"
            ),
            pytest.param(None, None, None, list(), None, None, None, None, id="clear genres"),
            pytest.param(
                None,
                None,
                None,
                ["education", "manual"],
                ["learning", "tutorial"],
                None,
                None,
                None,
                id="genres and keywords",
            ),
            pytest.param(None, None, None, None, None, None, 1024, None, id="page count only"),
            pytest.param(None, None, None, None, None, None, None, 2048, id="cover size only"),
            pytest.param(
                2016,
                "Yandex Sans",
                "other_editor_id",
                ["fun"],
                ["easy"],
                ["img"],
                512,
                4096,
                id="all",
            ),
            pytest.param(None, None, None, None, None, None, None, None, id="nothing"),
        ],
    )
    def test_update_index_field(
        self,
        update_book,
        book_id,
        commit_transaction,
        assert_one,
        year,
        font,
        editor,
        genres,
        keywords,
        illustrations,
        page_count,
        cover_size,
    ):
        update_book(
            book_id,
            year=year,
            font=font,
            editor=editor,
            genres=genres,
            keywords=keywords,
            illustrations=illustrations,
            page_count=page_count,
            cover_size=cover_size,
        )
        commit_transaction()
        assert_one(
            book_id,
            year=year,
            font=font,
            editor=editor,
            genres=genres,
            keywords=keywords,
            illustrations=illustrations,
            page_count=page_count,
            cover_size=cover_size,
        )

    def test_update_multiple_times(
        self,
        editor_id,
        update_book,
        book_id,
        paul_author_id,
        anna_author_id,
        commit_transaction,
        assert_one,
    ):
        years = (2014, 2015, 2016)
        fonts = ("Times New Roman", "Arial", "Comic Sans")
        editors = ("other_editor_id", editor_id, "other_editor_id")
        genres_list = ([], ["programming"], ["education"])
        keywords_list = (["learning"], [], ["python"])
        illustrations_list = (["imag3"], ["img4"], [])
        page_count_list = (512, 1024, 2048)
        author_ids_list = ([anna_author_id], [], [paul_author_id, anna_author_id])
        cover_size_list = (0, 1024, 512)
        for (
            year,
            font,
            editor,
            genres,
            keywords,
            illustrations,
            page_count,
            author_ids,
            cover_size,
        ) in zip(
            years,
            fonts,
            editors,
            genres_list,
            keywords_list,
            illustrations_list,
            page_count_list,
            author_ids_list,
            cover_size_list,
        ):
            update_book(
                book_id,
                year=year,
                font=font,
                editor=editor,
                genres=genres,
                keywords=keywords,
                illustrations=illustrations,
                page_count=page_count,
                author_ids=author_ids,
                cover_size=cover_size,
            )
        commit_transaction()
        assert_one(
            book_id,
            year=years[-1],
            font=fonts[-1],
            editor=editors[-1],
            genres=genres_list[-1],
            keywords=keywords_list[-1],
            illustrations=illustrations_list[-1],
            page_count=page_count_list[-1],
            author_ids=author_ids_list[-1],
            cover_size=cover_size_list[-1],
        )

    def test_create_remove(self, remove_book, book_id, commit_transaction, assert_empty):
        remove_book(book_id)
        commit_transaction()
        assert_empty()

    def test_create_update_remove(
        self,
        update_book,
        remove_book,
        book_id,
        commit_transaction,
        anna_author_id,
        assert_empty,
    ):
        update_book(
            book_id,
            name="updated name",
            year=2014,
            font="Times New Roman",
            genres=["education", "python"],
            keywords=["learning"],
            illustrations=["img2"],
            page_count=512,
            author_ids=[anna_author_id],
            cover_size=2048,
        )
        remove_book(book_id)
        commit_transaction()
        assert_empty()

    def test_create_remove_create(self, remove_book, commit_transaction, assert_one, create_book):
        year = 2014
        first_book_id = create_book(year=year)
        remove_book(first_book_id)
        second_book_id = create_book(year=year)
        commit_transaction()
        assert_one(year=year, book_id=second_book_id)

    def test_creation_time(self, example_env, commit_transaction, create_book):
        start_time = time.time()
        create_book()
        commit_transaction()
        finish_time = time.time()

        actual = example_env.get_table_rows(self.BOOKS_BY_CREATION_TIME_TABLE)
        assert len(actual) == 1

        creation_time = actual[0]["creation_time"]
        assert start_time * 1000000 <= creation_time
        assert finish_time * 1000000 >= creation_time

    ################################################################################

    @pytest.fixture
    def remove_transaction_id(self, example_env, book_id, commit_transaction):
        commit_transaction()  # For create book.
        transaction_id = example_env.client.start_transaction()
        example_env.client.remove_object("book", str(book_id), transaction_id=transaction_id)
        return transaction_id

    @pytest.fixture
    def update_transaction_id(self, example_env, book_id, commit_transaction):
        commit_transaction()  # For create book.
        transaction_id = example_env.client.start_transaction()
        example_env.client.update_object(
            "book",
            str(book_id),
            [{"path": "/spec/keywords", "value": ["learning", "python", "programming"]}],
            transaction_id=transaction_id,
        )
        return transaction_id

    def test_concurrent_update_and_remove(
        self,
        example_env,
        update_transaction_id,
        remove_transaction_id,
        book_id,
        publisher_id,
    ):
        example_env.client.commit_transaction(update_transaction_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            example_env.client.commit_transaction(remove_transaction_id)

        keywords = ["learning", "python", "programming"]
        assert [keywords] == example_env.client.get_object("book", str(book_id), ["/spec/keywords"])
        assert [
            {
                "hash": Any(),
                "keywords": keyword,
                "publisher_id": publisher_id,
                "book_id": int(book_id.split(';')[0]),
                "book_id2": int(book_id.split(';')[1]),
                "dummy": None,
            }
            for keyword in sorted(keywords)
        ] == sorted(example_env.get_table_rows(self.BOOKS_BY_KEYWORDS_TABLE), key=lambda x: x["keywords"])

    def test_concurrent_remove_and_update(
        self,
        example_env,
        remove_transaction_id,
        update_transaction_id,
        book_id,
        publisher_id,
        assert_empty,
    ):
        example_env.client.commit_transaction(remove_transaction_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            example_env.client.commit_transaction(update_transaction_id)

        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("book", str(book_id), ["/meta/id"])
        assert_empty()

    def test_concurrent_update_different_indexes(
        self,
        example_env,
        update_transaction_id,
        book_id,
    ):
        update_name_transaction_id = example_env.client.start_transaction()
        example_env.client.update_object(
            "book",
            str(book_id),
            [{"path": "/spec/name", "value": "New learning python book"}],
            transaction_id=update_name_transaction_id,
        )

        example_env.client.commit_transaction(update_transaction_id)
        example_env.client.commit_transaction(update_name_transaction_id)

    ################################################################################

    def test_unique_index_common(self, example_env):
        example_env.create_object("genre", meta={"id": "fantasy_id"}, spec={"name": "Fantasy"})
        example_env.create_object("genre", meta={"id": "education_id"}, spec={"name": "Education"})
        assert [
            {"name": "Education", "genre_id": "education_id"},
            {"name": "Fantasy", "genre_id": "fantasy_id"},
        ] == sorted(
            example_env.get_table_rows(self.GENRE_BY_NAME_TABLE),
            key=lambda row: row["name"],
        )

        with pytest.raises(UniqueValueAlreadyExists):
            example_env.create_object("genre", spec={"name": "Fantasy"})
        assert [
            {"name": "Education", "genre_id": "education_id"},
            {"name": "Fantasy", "genre_id": "fantasy_id"},
        ] == sorted(
            example_env.get_table_rows(self.GENRE_BY_NAME_TABLE),
            key=lambda row: row["name"],
        )

        example_env.client.update_object(
            "genre",
            "education_id",
            [{"path": "/spec/name", "value": "Learning"}],
        )
        example_env.create_object("genre", meta={"id": "another_id"}, spec={"name": "Education"})
        assert [
            {"name": "Education", "genre_id": "another_id"},
            {"name": "Fantasy", "genre_id": "fantasy_id"},
            {"name": "Learning", "genre_id": "education_id"},
        ] == sorted(
            example_env.get_table_rows(self.GENRE_BY_NAME_TABLE),
            key=lambda row: row["name"],
        )

    def test_unique_index_concurrent_create(self, example_env):
        first_transaction_id = example_env.client.start_transaction()
        second_transaction_id = example_env.client.start_transaction()
        example_env.create_object(
            "genre",
            spec={"name": "Unique"},
            transaction_id=first_transaction_id,
        )
        example_env.create_object(
            "genre",
            spec={"name": "Unique"},
            transaction_id=second_transaction_id,
        )
        example_env.client.commit_transaction(first_transaction_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            example_env.client.commit_transaction(second_transaction_id)

    def test_unique_index_transaction_concurrent_create(self, example_env):
        transaction_id = example_env.client.start_transaction()
        example_env.create_object(
            "genre",
            spec={"name": "Single transaction"},
            transaction_id=transaction_id,
        )
        example_env.create_object(
            "genre",
            spec={"name": "Single transaction"},
            transaction_id=transaction_id,
        )
        with pytest.raises(UniqueValueAlreadyExists):
            example_env.client.commit_transaction(transaction_id)

    def test_unique_index_transaction_create_update(self, example_env):
        transaction_id = example_env.client.start_transaction()
        genre_id = example_env.create_object(
            "genre",
            spec={"name": "Transaction create"},
            transaction_id=transaction_id,
        )
        for i in range(3):
            example_env.client.update_object(
                "genre",
                genre_id,
                set_updates=[{"path": "/spec/name", "value": "Transaction update #{}".format(i)}],
                transaction_id=transaction_id,
            )
        example_env.client.commit_transaction(transaction_id)
        assert [
            {"name": "Transaction update #2", "genre_id": genre_id},
        ] == example_env.get_table_rows(self.GENRE_BY_NAME_TABLE)

    @pytest.mark.parametrize("iteration_count", (1, 5))
    def test_unique_index_transaction_create_remove(self, example_env, iteration_count):
        transaction_id = example_env.client.start_transaction()
        for i in range(iteration_count):
            genre_id = example_env.create_object(
                "genre",
                spec={"name": "Transaction create"},
                transaction_id=transaction_id,
            )
            example_env.client.remove_object("genre", genre_id, transaction_id)
        example_env.client.commit_transaction(transaction_id)
        assert [] == example_env.get_table_rows(self.GENRE_BY_NAME_TABLE)

    @pytest.mark.parametrize("iteration_count", (1, 5))
    def test_unique_index_transaction_remove_create(self, example_env, iteration_count):
        genre_id = example_env.create_object("genre", spec={"name": "remove-create"})
        transaction_id = example_env.client.start_transaction()
        for i in range(iteration_count):
            example_env.client.remove_object("genre", genre_id, transaction_id)
            genre_id = example_env.create_object(
                "genre", spec={"name": "remove-create"}, transaction_id=transaction_id
            )
        example_env.client.commit_transaction(transaction_id)
        assert [{"name": "remove-create", "genre_id": genre_id}] == example_env.get_table_rows(
            self.GENRE_BY_NAME_TABLE
        )

    def test_unique_index_transaction_concurrent_update(self, example_env):
        genre_id_1 = example_env.create_object(
            "genre",
            spec={"name": "Transaction concurrent update #1"},
        )
        genre_id_2 = example_env.create_object(
            "genre",
            spec={"name": "Transaction concurrent update #2"},
        )

        transaction_id = example_env.client.start_transaction()
        example_env.client.update_object(
            "genre",
            genre_id_1,
            set_updates=[{"path": "/spec/name", "value": "Transaction concurrent update #3"}],
            transaction_id=transaction_id,
        )
        example_env.client.update_object(
            "genre",
            genre_id_2,
            set_updates=[{"path": "/spec/name", "value": "Transaction concurrent update #1"}],
            transaction_id=transaction_id,
        )
        example_env.client.commit_transaction(transaction_id)

        assert [
            {"name": "Transaction concurrent update #1", "genre_id": genre_id_2},
            {"name": "Transaction concurrent update #3", "genre_id": genre_id_1},
        ] == sorted(
            example_env.get_table_rows(self.GENRE_BY_NAME_TABLE),
            key=lambda row: row["name"],
        )

    @pytest.mark.parametrize("iteration_count", (1, 5))
    def test_unique_index_transaction_updates(self, example_env, iteration_count):
        genre_id = example_env.create_object(
            "genre",
            spec={"name": "Transaction updates first state"},
        )

        transaction_id = example_env.client.start_transaction()
        for i in range(iteration_count):
            for value in ("Transaction updates second state", "Transaction updates first state"):
                example_env.client.update_object(
                    "genre",
                    genre_id,
                    set_updates=[{"path": "/spec/name", "value": value}],
                    transaction_id=transaction_id,
                )
        example_env.client.commit_transaction(transaction_id)

        assert [
            {"name": "Transaction updates first state", "genre_id": genre_id},
        ] == example_env.get_table_rows(self.GENRE_BY_NAME_TABLE)
