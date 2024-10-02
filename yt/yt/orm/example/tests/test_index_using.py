from yt.orm.library.common import (
    IndexNotApplicable,
    NoSuchIndex,
    YtResponseError,
)

import pytest
import time


FILTER_BY_YEAR = "[/spec/year]>=1900 AND [/spec/year]<1910"


class TestIndexUsing:
    RESET_DB_BEFORE_TEST = False

    @pytest.fixture(scope="module")
    def start_time(self):
        return time.time()

    @pytest.fixture(scope="class")
    def create_book(self, test_environment, start_time):
        def _inner(
            year, genres, keywords, illustrations, editor_id, store_rating, cover_image, author_ids
        ):
            attributes = {
                "spec": {
                    "name": "Book name for index usage test",
                    "year": year,
                    "font": "Times New Roman",
                    "genres": genres,
                    "keywords": keywords,
                    "design": {
                        "cover": {
                            "hardcover": True,
                            "image": cover_image,
                            "size": 1024,
                            "dpi": 300,
                        },
                        "color": "white",
                        "illustrations": illustrations,
                    },
                    "page_count": 97,
                    "digital_data": {
                        "store_rating": store_rating,
                        "available_formats": [
                            {
                                "format": "epub",
                                "size": 11778382,
                            },
                            {
                                "format": "pdf",
                                "size": 12778383,
                            },
                        ],
                    },
                    "author_ids": author_ids,
                },
                "meta": {
                    "isbn": "978-1449355739",
                    "parent_key": test_environment.client.create_object(
                        "publisher",
                        {"spec": {"name": "O'REILLY"}},
                        request_meta_response=True,
                    ),
                },
            }
            if editor_id is not None:
                attributes["spec"]["editor_id"] = test_environment.client.create_object(
                    "editor", {"meta": {"id": editor_id}, "spec": {"name": "Greg"}}
                )
            return test_environment.client.create_object(
                "book",
                attributes=attributes,
                request_meta_response=True,
                enable_structured_response=True,
            )["meta"]["key"]

        return _inner

    @pytest.fixture(scope="class")
    def created_books(self, test_environment, create_book):
        greg_id = test_environment.client.create_object(
            "author", {"meta": {"id": "greg_id"}, "spec": {"name": "Greg"}}
        )
        anna_id = test_environment.client.create_object(
            "author", {"meta": {"id": "anna_id"}, "spec": {"name": "Anna"}}
        )

        years = range(1900, 1910)
        genres_list = [
            ["first_genre", "second_genre"] if i >= 5 else ["unique_genre"] for i in range(10)
        ]
        keywords_list = [
            ["first keyword", "second keyword"] if i % 2 else ["unique keyword"] for i in range(10)
        ]
        illustrations_list = [
            ["first/image", "second/image"] if i >= 2 else ["unique/image"] for i in range(10)
        ]
        editor_id_list = ["editor_{}".format(i) if i != 7 else None for i in range(10)]
        store_rating_list = [i / 10.0 for i in range(40, 50)]
        cover_image_list = ["cover_{}.png".format(i) for i in range(10)]
        author_ids_list = [[anna_id] if i % 2 else [anna_id, greg_id] for i in range(10)]
        return [
            [
                create_book(
                    year,
                    genres,
                    keywords,
                    illustrations,
                    editor_id,
                    store_rating,
                    cover_image,
                    author_ids,
                )
            ]
            for year, genres, keywords, illustrations, editor_id, store_rating, cover_image, author_ids in zip(
                years,
                genres_list,
                keywords_list,
                illustrations_list,
                editor_id_list,
                store_rating_list,
                cover_image_list,
                author_ids_list,
            )
        ]

    @pytest.fixture
    def select_objects(self, example_env):
        def _inner(
            filter,
            selectors=["/meta/key"],
            index=None,
            offset=None,
            limit=None,
            options=None,
            order_by=None,
            enable_structured_response=False,
        ):
            return example_env.client.select_objects(
                "book",
                filter=filter,
                selectors=selectors,
                index=index,
                offset=offset,
                limit=limit,
                options=options,
                order_by=order_by,
                enable_structured_response=enable_structured_response,
            )

        return _inner

    def test_no_such_index(self, select_objects):
        with pytest.raises(NoSuchIndex):
            select_objects(filter="[/spec/year]=1900", index="unknown_index")

    @pytest.mark.parametrize(
        "index",
        [
            "books_by_font",
            "books_by_editor",
            "books_by_store_rating",
            "books_by_cover_image",
            "books_by_genres",
            "books_by_keywords",
            "books_by_authors",
            "books_by_isbn",
            "books_by_cover_size",
            "books_by_cover_dpi",
        ],
    )
    def test_no_index_field_in_filter_fail(self, select_objects, index):
        with pytest.raises(IndexNotApplicable):
            select_objects(filter=FILTER_BY_YEAR, index=index)

    @pytest.mark.parametrize("index", [None, "editors_by_name"])
    def test_annotations(self, example_env, index):
        name = "Name fo case {}".format(index)
        resume = "Resume fo case {}".format(index)

        example_env.client.create_object(
            "editor",
            attributes={"spec": {"name": name}, "annotations": {"resume": resume}},
        )
        actual = example_env.client.select_objects(
            "editor",
            selectors=["/annotations/resume"],
            filter='[/spec/name] = "{}"'.format(name),
            index=index,
        )
        assert [[resume]] == actual

    class TestScalarIndexUsing:
        RESET_DB_BEFORE_TEST = False
        YEAR_INDEXES = [
            None,
            "books_by_year",
            "books_by_year_and_font",
        ]

        @pytest.mark.parametrize("index", YEAR_INDEXES)
        def test_select_one(self, select_objects, created_books, index):
            actual = select_objects(filter="[/spec/year]=1900", index=index)
            assert created_books[:1] == actual

        @pytest.mark.parametrize("index", YEAR_INDEXES)
        def test_select_all(self, select_objects, created_books, index):
            actual = select_objects(filter=FILTER_BY_YEAR, index=index)
            assert sorted(created_books) == sorted(actual)

        @pytest.mark.parametrize("index", YEAR_INDEXES)
        def test_select_in(self, select_objects, created_books, index):
            actual = select_objects(filter="[/spec/year] in (1901, 1902, 1903)", index=index)
            assert sorted(created_books[1:4]) == sorted(actual)

        @pytest.mark.parametrize("index", YEAR_INDEXES)
        def test_select_empty(self, select_objects, created_books, index):
            actual = select_objects(filter="[/spec/year]=3000", index=index)
            assert [] == actual

        @pytest.mark.parametrize("index", YEAR_INDEXES)
        def test_scalar_ordered_pagination(
            self,
            select_objects,
            created_books,
            index,
        ):
            actual_first_batch = select_objects(
                filter=FILTER_BY_YEAR,
                index=index,
                limit=3,
                offset=2,
                enable_structured_response=True,
                order_by=[{"expression": "/spec/year"}],
            )
            assert created_books[2:5] == [[i[0]["value"]] for i in actual_first_batch["results"]]
            assert actual_first_batch["continuation_token"]

            actual_second_batch = select_objects(
                filter=FILTER_BY_YEAR,
                index=index,
                limit=3,
                options={
                    "continuation_token": actual_first_batch["continuation_token"],
                },
                enable_structured_response=True,
                order_by=[{"expression": "/spec/year"}],
            )
            assert created_books[5:8] == [[i[0]["value"]] for i in actual_second_batch["results"]]

        @pytest.mark.parametrize("index", YEAR_INDEXES)
        @pytest.mark.parametrize("limit", [1, 3, 100])
        def test_scalar_continuation_read(
            self,
            select_objects,
            created_books,
            index,
            limit,
        ):
            continuation_token = None
            actual = []
            while True:
                response = select_objects(
                    filter=FILTER_BY_YEAR,
                    options={
                        "continuation_token": continuation_token,
                    },
                    index=index,
                    limit=limit,
                    enable_structured_response=True,
                )
                continuation_token = response["continuation_token"]
                response_values = [[item[0]["value"]] for item in response["results"]]
                actual = actual + response_values
                if len(response_values) < limit:
                    break
            assert sorted(created_books) == sorted(actual)

        @pytest.mark.parametrize("index", [None, "books_by_editor"])
        def test_one_to_many_full(self, select_objects, index, created_books):
            actual = select_objects(
                filter='[/spec/editor_id]="editor_8" AND {}'.format(FILTER_BY_YEAR),
                index=index,
            )
            assert [created_books[8]] == actual

        @pytest.mark.parametrize(
            "index",
            [
                None,
                pytest.param(
                    "books_by_editor", marks=pytest.mark.xfail
                ),  # TODO: unlock after YTORM-259 fixed
                "books_by_editor_and_year_with_predicate",
            ],
        )
        def test_one_to_many_empty(self, select_objects, index, created_books):
            actual = select_objects(
                filter='[/spec/editor_id]="" AND {}'.format(FILTER_BY_YEAR),
                index=index,
            )
            assert [created_books[7]] == actual

        @pytest.mark.parametrize("index", [None, "books_by_store_rating"])
        def test_nested_field_separate_column(self, select_objects, index, created_books):
            actual = select_objects(
                filter="[/spec/digital_data/store_rating] > 4.0 AND [/spec/digital_data/store_rating] < 4.2",
                index=index,
            )
            assert [created_books[1]] == actual

        @pytest.mark.parametrize("index", [None, "books_by_cover_image"])
        def test_nested_field_etc_column(self, select_objects, index, created_books):
            actual = select_objects(filter='[/spec/design/cover/image]="cover_4.png"', index=index)
            assert [created_books[4]] == actual

        @pytest.mark.parametrize(
            "index",
            [
                None,
                "books_by_year",
                "books_by_font",
                "books_by_year_and_font",
                "books_by_editor",
                "books_by_store_rating",
                "books_by_cover_image",
                "books_by_isbn",
                "books_by_cover_size",
                "books_by_cover_dpi",
            ],
        )
        @pytest.mark.parametrize(
            "selectors,expected_data",
            [
                pytest.param(
                    [
                        "/spec/name",
                        "/meta/isbn",
                        "/spec/page_count",
                        "/spec/design/cover/hardcover",
                    ],
                    [["Book name for index usage test", "978-1449355739", 97, True]],
                    id="constant fields",
                ),
                pytest.param(
                    ["/spec/year", "/spec/design/cover/image"],
                    [[1900, "cover_0.png"]],
                    id="scalar fields",
                ),
                pytest.param(
                    ["/spec/genres", "/spec/keywords"],
                    [[["unique_genre"], ["unique keyword"]]],
                    id="repeated fields",
                ),
            ],
        )
        def test_scalar_selectors(self, created_books, select_objects, selectors, index, expected_data):
            actual = select_objects(
                filter=" AND ".join(
                    [
                        "[/spec/year]>=1900",
                        "[/spec/digital_data/store_rating]>=4.0",
                        '[/spec/editor_id]="editor_0"',
                        '[/spec/design/cover/image]="cover_0.png"',
                        '[/spec/font]="Times New Roman"',
                        '[/meta/isbn]="978-1449355739"',
                        "[/spec/design/cover/size]=1024u",
                        "[/spec/design/cover/dpi]=300",
                    ]
                ),
                selectors=selectors,
                index=index,
            )
            assert expected_data == actual

        @pytest.mark.parametrize("index", YEAR_INDEXES + ["books_by_store_rating"])
        def test_scalar_order_by(self, select_objects, index, created_books):
            actual = select_objects(
                filter=" AND ".join(
                    ["[/spec/year]>=1902", "[/spec/digital_data/store_rating]<=4.5"]
                ),
                index=index,
                order_by=[
                    {"expression": "/spec/year", "descending": True},
                    {"expression": "/spec/editor_id"},
                ],
            )
            assert created_books[5:1:-1] == actual

        def test_index_mode_store(self, select_objects, example_env):
            with pytest.raises(YtResponseError):
                select_objects(filter="[/spec/page_count]=97u", index="books_by_page_count")

            config = {"object_manager": {"indexes_with_allowed_building_read": ["books_by_page_count"]}}
            with example_env.set_cypress_config_patch_in_context(config):
                select_objects(filter="[/spec/page_count]=97u", index="books_by_page_count")

        def test_index_mode_none(self, select_objects):
            with pytest.raises(YtResponseError):
                select_objects(
                    filter='[/spec/name]="Book name for index usage test"', index="books_by_name"
                )

        @pytest.mark.parametrize("index", [None, "typographers_by_login"])
        def test_scalar_index_meta_field(self, example_env, index):
            typographer_id = example_env.client.create_object(
                "typographer",
                attributes={
                    "meta": {
                        "login": "printer_{}".format(index),
                        "inn": "123_345_789-{}".format(index),
                        "parent_key": example_env.client.create_object(
                            "publisher",
                            {"spec": {"name": "O'REILLY"}},
                            request_meta_response=True,
                        ),
                    },
                    "spec": {
                        "test_mandatory_etc_field": "first",
                        "test_mandatory_column_field": "second",
                    },
                },
                request_meta_response=True,
                enable_structured_response=True,
            )["meta"]["id"]
            actual = example_env.client.select_objects(
                "typographer",
                filter='[/meta/login]="printer_{}"'.format(index),
                selectors=["/meta/id"],
                index=index,
            )
            assert [[typographer_id]] == actual

        @pytest.mark.parametrize("index", [None, "books_by_creation_time"])
        def test_creation_time(self, index, select_objects, start_time, created_books):
            actual = select_objects(
                filter="[/meta/creation_time]>={} AND [/meta/creation_time]<={}".format(
                    int(start_time * 1000000),
                    int(time.time() * 1000000),
                ),
                index=index,
            )
            assert sorted(created_books) == sorted(actual)

    class TestRepeatedIndexUsing:
        RESET_DB_BEFORE_TEST = False
        INDEXES = [
            None,
            "books_by_genres",
            "books_by_keywords",
            "books_by_illustrations",
            "books_by_authors",
        ]
        FILTER_TEMPLATE = " AND ".join(
            [
                'list_contains([/spec/genres], "{genre}")',
                'list_contains([/spec/keywords], "{keyword}")',
                'list_contains([/spec/design/illustrations], "{illustration}")',
                'list_contains([/spec/author_ids], "{author_id}")',
                FILTER_BY_YEAR,
            ]
        )

        @pytest.mark.parametrize("index", INDEXES)
        def test_one_of_several_items(self, select_objects, index, created_books):
            actual = select_objects(
                filter=self.FILTER_TEMPLATE.format(
                    genre="first_genre",
                    keyword="first keyword",
                    illustration="first/image",
                    author_id="anna_id",
                ),
                index=index,
            )
            assert sorted(created_books[5::2]) == sorted(actual)

        @pytest.mark.parametrize("index", INDEXES)
        def test_unique_item(self, select_objects, index, created_books):
            actual = select_objects(
                filter=self.FILTER_TEMPLATE.format(
                    genre="unique_genre",
                    keyword="unique keyword",
                    illustration="unique/image",
                    author_id="greg_id",
                ),
                index=index,
            )
            assert sorted(created_books[:2:2]) == sorted(actual)

        @pytest.mark.parametrize("index", INDEXES)
        def test_unknown_item(self, select_objects, index, created_books):
            actual = select_objects(
                filter=self.FILTER_TEMPLATE.format(
                    genre="unknown_genre",
                    keyword="unknown_keyword",
                    illustration="unknown/image",
                    author_id="unwnown_author",
                ),
                index=index,
            )
            assert [] == actual

        @pytest.mark.parametrize("index", [None, "books_by_genres"])
        def test_unique_row(self, select_objects, created_books, index):
            actual = select_objects(
                filter="".join(
                    [
                        '(list_contains([/spec/genres], "first_genre") OR ',
                        'list_contains([/spec/genres], "second_genre")) AND ',
                        FILTER_BY_YEAR,
                    ]
                ),
                index=index,
            )
            assert sorted(created_books[5:]) == sorted(actual)

        @pytest.mark.parametrize(
            "index",
            [
                None,
                "books_by_genres",
            ],
        )
        def test_repeated_ordered_pagination(
            self,
            select_objects,
            created_books,
            index,
        ):
            filter = "".join(
                [
                    '(list_contains([/spec/genres], "first_genre") OR ',
                    'list_contains([/spec/genres], "second_genre")) AND ',
                    FILTER_BY_YEAR,
                ]
            )
            actual_first = select_objects(
                filter=filter,
                index=index,
                limit=2,
                offset=1,
                enable_structured_response=True,
                order_by=[{"expression": "/spec/year"}],
            )
            assert created_books[6:8] == [[i[0]["value"]] for i in actual_first["results"]]
            assert actual_first["continuation_token"]

            actual_second = select_objects(
                filter=filter,
                index=index,
                limit=2,
                options={
                    "continuation_token": actual_first["continuation_token"],
                },
                enable_structured_response=True,
                order_by=[{"expression": "/spec/year"}],
            )
            assert created_books[8:] == [[i[0]["value"]] for i in actual_second["results"]]

        @pytest.mark.parametrize(
            "index",
            [
                None,
                "books_by_keywords",
            ],
        )
        @pytest.mark.parametrize("limit", [1, 3, 100])
        def test_repeated_continuation_read(
            self,
            select_objects,
            created_books,
            index,
            limit,
        ):
            continuation_token = None
            actual = []
            while True:
                response = select_objects(
                    filter="".join(
                        [
                            '(list_contains([/spec/keywords], "first keyword") OR ',
                            'list_contains([/spec/keywords], "second keyword")) AND ',
                            FILTER_BY_YEAR,
                        ]
                    ),
                    options={
                        "continuation_token": continuation_token,
                    },
                    index=index,
                    limit=limit,
                    enable_structured_response=True,
                )
                response_values = [[i[0]["value"]] for i in response["results"]]
                continuation_token = response["continuation_token"]
                actual = actual + response_values
                if len(response_values) < limit:
                    break
            assert sorted(created_books[1::2]) == sorted(actual)

        @pytest.mark.parametrize("index", [None, "books_by_genres"])
        def test_duplicate_query(self, select_objects, created_books, index):
            actual = select_objects(
                filter="".join(
                    [
                        '(list_contains([/spec/genres], "unique_genre") OR ',
                        'list_contains([/spec/genres], "unique_genre")) AND ',
                        FILTER_BY_YEAR,
                    ]
                ),
                index=index,
            )
            assert sorted(created_books[:5]) == sorted(actual)

        @pytest.mark.parametrize(
            "index",
            [
                None,
                "books_by_year",
                "books_by_year_and_font",
            ],
        )
        def test_no_suitable_scalar_index(self, select_objects, created_books, index):
            actual = select_objects(
                filter='list_contains([/spec/genres], "first_genre") AND ' + FILTER_BY_YEAR,
                index=index,
            )
            assert sorted(created_books[5:]) == sorted(actual)

        def test_no_suitable_repeated_index(self, select_objects, created_books):
            with pytest.raises(IndexNotApplicable):
                select_objects(
                    filter='list_contains([/spec/genres], "first_genre") AND ' + FILTER_BY_YEAR,
                    index="books_by_keywords",
                )

        @pytest.mark.parametrize("index", INDEXES)
        def test_first_arg_no_reference(self, select_objects, index):
            with pytest.raises(YtResponseError):
                select_objects(
                    filter="list_contains(any=[1; 2; 3], 2)",
                    index=index,
                )

        def test_second_arg_no_literal(self, select_objects):
            with pytest.raises(YtResponseError):
                select_objects(
                    filter="list_contains([/spec/genres], [/spec/name])",
                    index="books_by_genres",
                )

        @pytest.mark.parametrize("index", INDEXES)
        @pytest.mark.parametrize(
            "selectors,expected_data",
            [
                pytest.param(
                    [
                        "/spec/year",
                        "/spec/name",
                        "/spec/design/cover/hardcover",
                        "/spec/design/color",
                    ],
                    [[1905, "Book name for index usage test", True, "white"]],
                    id="several fields",
                ),
                pytest.param(
                    ["/spec/year", "/spec/year", "/spec/design/color", "/spec/design/color"],
                    [[1905, 1905, "white", "white"]],
                    id="duplicate",
                ),
                pytest.param(
                    [
                        "/spec/genres",
                        "/spec/keywords",
                        "/spec/design/illustrations",
                        "/spec/author_ids",
                    ],
                    [
                        [
                            ["first_genre", "second_genre"],
                            ["first keyword", "second keyword"],
                            ["first/image", "second/image"],
                            ["anna_id"],
                        ]
                    ],
                    id="filtered fields",
                ),
                pytest.param([], [[]], id="empty"),
            ],
        )
        def test_repeated_selectors(
            self, select_objects, created_books, index, selectors, expected_data
        ):
            actual = select_objects(
                filter=" AND ".join(
                    [
                        'list_contains([/spec/genres], "first_genre")',
                        'list_contains([/spec/keywords], "first keyword")',
                        'list_contains([/spec/design/illustrations], "first/image")',
                        'list_contains([/spec/author_ids], "anna_id")',
                        "[/spec/year]=1905",
                    ]
                ),
                index=index,
                selectors=selectors,
            )
            assert expected_data == actual

        @pytest.mark.parametrize("index", INDEXES)
        def test_repeated_order_by(self, select_objects, index, created_books):
            actual = select_objects(
                filter=self.FILTER_TEMPLATE.format(
                    genre="first_genre",
                    keyword="first keyword",
                    illustration="first/image",
                    author_id="anna_id",
                ),
                index=index,
                order_by=[{"expression": "/spec/year"}],
            )
            assert created_books[5::2] == actual
