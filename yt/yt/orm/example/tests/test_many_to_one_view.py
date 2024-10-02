import pytest


class TestManyToOneView:
    @pytest.mark.xfail
    def test_create(self, example_env):
        publisher_key = example_env.client.create_object("publisher", request_meta_response=True)
        example_env.client.create_object(
            "book",
            {
                "meta": {"isbn": "978-1449355739", "parent_key": publisher_key},
                "spec": {"editor": {"spec": {"name": "Mort"}}},
            },
        )

    @pytest.mark.xfail
    def test_set_update(self, example_env):
        publisher = example_env.create_publisher()
        editor = example_env.create_editor()
        book = example_env.create_book(publisher, spec={"editor_id": str(editor)})
        example_env.client.update_object(
            "book",
            str(book),
            set_updates=[{"path": "/spec/editor/spec/name", "value": "Mort"}],
        )

    @pytest.mark.xfail
    def test_remove_update(self, example_env):
        publisher = example_env.create_publisher()
        editor = example_env.create_editor()
        book = example_env.create_book(publisher, spec={"editor_id": str(editor)})
        example_env.client.update_object(
            "book",
            str(book),
            remove_updates=[{"path": "/spec/editor/spec/name"}],
        )

    @pytest.mark.xfail
    def test_lock_update(self, example_env):
        publisher = example_env.create_publisher()
        editor = example_env.create_editor()
        book = example_env.create_book(publisher, spec={"editor_id": str(editor)})
        example_env.client.update_object(
            "book",
            str(book),
            lock_updates=[{"path": "/spec/editor/spec/name", "lock_type": "shared_strong"}],
        )

    @pytest.mark.parametrize("has_foreign", [False, True])
    @pytest.mark.parametrize("objects_count", [1, 2, 10])
    def test_performance(self, example_env, objects_count, has_foreign):
        publisher = example_env.create_publisher()
        books = []
        for _ in range(objects_count):
            spec = {}
            if has_foreign:
                illustrator = example_env.create_illustrator()
                spec["illustrator_id"] = illustrator
            book = example_env.create_book(publisher, spec=spec)
            books.append(book)

        performance_statistics = example_env.client.get_objects(
            "book",
            [str(book) for book in books],
            selectors=["/spec/illustrator/meta/uid", "/spec/illustrator/spec", "/spec/illustrator/spec/name"],
            common_options={"fetch_performance_statistics": True},
            enable_structured_response=True,
        )["performance_statistics"]
        if has_foreign:
            assert 6 == performance_statistics["read_phase_count"]
        else:
            assert 4 == performance_statistics["read_phase_count"]

    def test_get_empty(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)
        example_env.client.get_objects(
            "book",
            [str(book)],
            selectors=["/spec/illustrator/meta/uid", "/spec/illustrator/spec", "/spec/cover_illustrator/spec/name", "/spec/illustrator_id"],
        )

    def test_get_meta_fields(self, example_env):
        publisher = example_env.create_publisher()
        illustrator = example_env.create_illustrator(name="Mort")
        book = example_env.create_book(publisher, spec={"illustrator_id": illustrator})
        book_response = example_env.client.get_objects(
            "book",
            [str(book)],
            selectors=["/spec/illustrator/meta/type", "/spec/illustrator/meta/creation_time"],
            enable_structured_response=True,
        )["results"][0]
        assert book_response[0]["value"] == "illustrator"
        assert isinstance(book_response[1]["value"], int)

    def test_get(self, example_env):
        publisher = example_env.create_publisher()
        part_time_job = example_env.create_publisher()
        illustrator = example_env.create_illustrator(name="Mort", part_time_job=part_time_job)
        cover_illustrator = example_env.create_illustrator(name="Alex")
        book = example_env.create_book(publisher, spec={"illustrator_id": illustrator, "cover_illustrator_id": cover_illustrator})
        book_response = example_env.client.get_objects(
            "book",
            [str(book)],
            selectors=["/spec/illustrator/meta/uid", "/spec/illustrator/spec", "/spec/cover_illustrator/spec/name", "/spec/illustrator_id"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"]
        illustrators_response = example_env.client.get_objects(
            "illustrator",
            [str(illustrator), str(cover_illustrator)],
            selectors=["/meta/uid", "/spec", "/spec/name"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"]
        assert book_response[0][:2] == illustrators_response[0][:2]
        assert book_response[0][2] == illustrators_response[1][2]
        result = example_env.client.get_object(
            "illustrator",
            str(illustrator),
            selectors=["/meta/part_time_job_view/meta/id"],
        )
        assert result[0] == part_time_job

    def test_get_fetch_root_object_and_spec(self, example_env):
        publisher = example_env.create_publisher()
        part_time_job = example_env.create_publisher()
        illustrator = example_env.create_illustrator(name="Mort", part_time_job=part_time_job)
        cover_illustrator = example_env.create_illustrator(name="Alex")
        book = example_env.create_book(publisher, spec={"illustrator_id": illustrator, "cover_illustrator_id": cover_illustrator})
        book_response = example_env.client.get_objects(
            "book",
            [str(book)],
            selectors=["/spec/illustrator/spec", "/spec/illustrator/meta", "/spec"],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"][0][0]
        illustrator_response = example_env.client.get_objects(
            "illustrator",
            [str(illustrator), str(cover_illustrator)],
            selectors=["/meta", "/spec"],
            enable_structured_response=True,
            options={"fetch_root_object": True},
        )["results"][0][0]
        assert book_response["value"]["spec"]["illustrator"] == illustrator_response["value"]

    def test_select(self, example_env):
        publisher = example_env.create_publisher()
        part_time_job = example_env.create_publisher()
        illustrator = example_env.create_illustrator(name="Mort", part_time_job=part_time_job)
        cover_illustrator = example_env.create_illustrator(name="Alex")
        example_env.create_book(
            publisher, spec={"illustrator_id": illustrator, "cover_illustrator_id": cover_illustrator, "year": 1913}
        )
        book_response = example_env.client.select_objects(
            "book",
            filter="[/spec/year]=1913",
            index="books_by_year",
            selectors=["/spec/illustrator/meta/uid", "/spec/illustrator/spec", "/spec/cover_illustrator/spec/name", "/spec/illustrator_id"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"]
        illustrators_response = example_env.client.get_objects(
            "illustrator",
            [str(illustrator), str(cover_illustrator)],
            selectors=["/meta/uid", "/spec", "/spec/name"],
            enable_structured_response=True,
            options={"fetch_timestamps": True},
        )["results"]
        assert book_response[0][:2] == illustrators_response[0][:2]
        assert book_response[0][2] == illustrators_response[1][2]
