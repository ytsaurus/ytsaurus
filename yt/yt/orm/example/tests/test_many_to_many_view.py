import pytest


class TestManyToManyView:
    @pytest.mark.xfail
    def test_create(self, example_env):
        publisher_key = example_env.client.create_object("publisher", request_meta_response=True)
        example_env.client.create_object(
            "book",
            {
                "meta": {"isbn": "978-1449355739", "parent_key": publisher_key},
                "spec": {"authors": [{"spec": {"name": "Mort"}}]},
            },
        )

    @pytest.mark.xfail
    def test_update(self, example_env):
        publisher = example_env.create_publisher()
        author = example_env.create_author()
        book = example_env.create_book(publisher, spec={"author_ids": [author]})
        example_env.client.update_object(
            "book", str(book), set_updates=[{"path": "/spec/authors/*/spec/name", "value": "Mort"}]
        )

    @pytest.mark.parametrize("foreign_count", [0, 1, 10])
    @pytest.mark.parametrize("objects_count", [1, 2, 10])
    def test_performance(self, example_env, objects_count, foreign_count):
        publisher = example_env.create_publisher()
        books = []
        for _ in range(objects_count):
            authors = (
                example_env.client.create_objects((("author", dict()) for _ in range(foreign_count)))
                if foreign_count
                else []
            )

            book = example_env.create_book(publisher, spec={"author_ids": authors})
            books.append(book)
        performance_statistics = example_env.client.get_objects(
            "book",
            [str(book) for book in books],
            selectors=["/spec/authors/*/meta/id", "/spec/authors/*/spec", "/spec/authors/*/spec/name"],
            common_options={"fetch_performance_statistics": True},
            enable_structured_response=True,
        )["performance_statistics"]
        if foreign_count > 0:
            assert 6 == performance_statistics["read_phase_count"]
        else:
            assert 4 == performance_statistics["read_phase_count"]

    def test_get(self, example_env):
        publisher = example_env.create_publisher()
        author1 = example_env.create_author(name="Mort")
        author2 = example_env.create_author(name="Alex")
        book = example_env.create_book(publisher, spec={"author_ids": [author1, author2]})
        book_response = example_env.client.get_objects(
            "book",
            [str(book)],
            selectors=["/spec/authors/*/meta/id", "/spec/authors/*/spec", "/spec/authors/*/spec/name"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"][0]
        authors_response = example_env.client.get_objects(
            "author",
            [str(author1), str(author2)],
            selectors=["/meta/id", "/spec", "/spec/name"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"]
        for i, attribute in enumerate(book_response):
            expected_value = [author[i]["value"] for author in authors_response]
            expected_timestamp = max(author[i]["timestamp"] for author in authors_response)
            assert attribute["value"] == expected_value
            assert attribute["timestamp"] == expected_timestamp

    def test_select(self, example_env):
        publisher = example_env.create_publisher()
        author1 = example_env.create_author(name="Mort")
        author2 = example_env.create_author(name="Alex")
        example_env.create_book(publisher, spec={"author_ids": [author1, author2], "year": 1914})
        book_response = example_env.client.select_objects(
            "book",
            filter="[/spec/year]=1914",
            index="books_by_year",
            selectors=["/spec/authors/*/meta/id", "/spec/authors/*/spec", "/spec/authors/*/spec/name"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"][0]
        authors_response = example_env.client.get_objects(
            "author",
            [str(author1), str(author2)],
            selectors=["/meta/id", "/spec", "/spec/name"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"]
        for i, attribute in enumerate(book_response):
            expected_value = [author[i]["value"] for author in authors_response]
            expected_timestamp = max(author[i]["timestamp"] for author in authors_response)
            assert attribute["value"] == expected_value
            assert attribute["timestamp"] == expected_timestamp

    @pytest.mark.parametrize("path", ["", "/spec", "/spec/name"])
    def test_get_fetch_root_object(self, path, example_env):
        publisher = example_env.create_publisher()
        author1 = example_env.create_author(name="Mort")
        author2 = example_env.create_author(name="Alex")
        book = example_env.create_book(publisher, spec={"author_ids": [author1, author2]})
        authors_response = example_env.client.get_objects(
            "author",
            [str(author1), str(author2)],
            selectors=[path],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"]
        book_response = example_env.client.get_objects(
            "book",
            [str(book)],
            selectors=[f"/spec/authors/*{path}"],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"][0]
        for i, attribute in enumerate(book_response):
            expected_value = [author[i]["value"] for author in authors_response]
            assert attribute["value"]["spec"]["authors"] == expected_value

    @pytest.mark.parametrize("path", ["", "/spec", "/spec/name"])
    def test_select_fetch_root_object(self, path, example_env):
        publisher = example_env.create_publisher()
        author1 = example_env.create_author(name="Mort")
        author2 = example_env.create_author(name="Alex")
        example_env.create_book(publisher, spec={"author_ids": [author1, author2], "year": 1922})
        authors_response = example_env.client.get_objects(
            "author",
            [str(author1), str(author2)],
            selectors=[path],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"]
        book_response = example_env.client.select_objects(
            "book",
            filter="[/spec/year]=1922",
            index="books_by_year",
            selectors=[f"/spec/authors/*{path}"],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"][0]
        for i, attribute in enumerate(book_response):
            expected_value = [author[i]["value"] for author in authors_response]
            assert attribute["value"]["spec"]["authors"] == expected_value

    @pytest.mark.parametrize("path", ["", "/spec", "/spec/name"])
    def test_get_fetch_root_object_with_composite(self, path, example_env):
        publisher = example_env.create_publisher()
        author1 = example_env.create_author(name="Mort")
        author2 = example_env.create_author(name="Alex")
        book = example_env.create_book(publisher, spec={"name": "Яндекс.Книга", "author_ids": [author1, author2]})
        authors_response = example_env.client.get_objects(
            "author",
            [str(author1), str(author2)],
            selectors=[path],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"]
        book_response = example_env.client.get_objects(
            "book",
            [str(book)],
            selectors=[f"/spec/authors/*{path}", "/spec"],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"][0][0]["value"]
        assert book_response["spec"]["name"] == "Яндекс.Книга"
        expected_value = [author[0]["value"] for author in authors_response]
        assert book_response["spec"]["authors"] == expected_value
