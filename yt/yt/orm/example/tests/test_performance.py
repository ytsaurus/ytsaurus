from .conftest import sync_access_control, create_user_client

from yt.orm.library.common import wait

import pytest

from datetime import timedelta


def get_sanitizer_type():
    import yatest.common

    try:
        sanitizer = yatest.common.context.sanitize
        if sanitizer == "":
            return None
    except NotImplementedError:
        return None


@pytest.mark.skipif(
    get_sanitizer_type() is not None, reason="Skip performance tests in build with sanitizers"
)
class TestPerformance:
    EXAMPLE_MASTER_CONFIG = {
        "transaction_manager": {
            "allow_parent_removal_override": True,
        },
    }

    def _revoke_all_schema_permissions_for(self, example_env, object_type):
        example_env.client.update_object(
            "schema",
            object_type,
            set_updates=[{"path": "/meta/acl", "value": []}],
        )

    def _pick_authors(self, author_ids):
        author_ids = author_ids * 3
        while author_ids:
            yield [author_ids.pop(), author_ids.pop(), author_ids.pop()]

    def test_get_select(self, example_env):
        publisher_id = example_env.create_publisher()

        book_id = example_env.create_book(publisher_id=publisher_id)

        # Phases: parent key, requested fields, /meta evaluated attributes.
        performance_statistics = example_env.client.get_object(
            "book",
            str(book_id),
            selectors=["/meta", "/spec", "/status"],
            common_options=dict(fetch_performance_statistics=True),
            enable_structured_response=True,
        )["performance_statistics"]

        assert 4 == performance_statistics["read_phase_count"]
        assert 1 == len(performance_statistics["select_query_statistics"])
        assert (
            performance_statistics["select_query_statistics"][0]["table_name"]
            == "//home/example/db/books"
        )
        assert 1 == len(performance_statistics["select_query_statistics"][0]["statistics"])
        assert (
            performance_statistics["select_query_statistics"][0]["statistics"][0]["rows_read"]
            == performance_statistics["select_query_statistics"][0]["statistics"][0]["rows_written"]
            == 1
        )

        # Phases: parent key, requested fields.
        assert (
            3
            == example_env.client.get_object(
                "book",
                str(book_id),
                selectors=["/spec", "/status"],
                common_options=dict(fetch_performance_statistics=True),
                enable_structured_response=True,
            )["performance_statistics"]["read_phase_count"]
        )

        # Phases: requested fields.
        assert (
            1
            == example_env.client.select_objects(
                "book",
                filter="[/meta/publisher_id] = {} and [/meta/id] = {} and [/meta/id2] = {}".format(
                    publisher_id, book_id.split(";")[0], book_id.split(";")[1]
                ),
                selectors=["/spec", "/status"],
                common_options=dict(fetch_performance_statistics=True),
                enable_structured_response=True,
            )["performance_statistics"]["read_phase_count"]
        )

    def test_batched_select(self, test_environment, example_env):
        self._revoke_all_schema_permissions_for(example_env, "book")
        example_env.create_books_with_publishers(count=4)

        with create_user_client(test_environment) as client:
            # Phases: select, book acl and parent_key, book schema, publisher acl, publisher schema.
            assert (
                5
                == client.select_objects(
                    "book",
                    selectors=["/meta/id"],
                    common_options=dict(fetch_performance_statistics=True),
                    enable_structured_response=True,
                )["performance_statistics"]["read_phase_count"]
            )

    def test_batched_get(self, test_environment, example_env):
        self._revoke_all_schema_permissions_for(example_env, "book")
        book_keys = example_env.create_books_with_publishers(count=6)

        with create_user_client(test_environment) as client:
            # Phases: existence_checks, book acl and parent_key, book schema, publisher acl, publisher schema, selector
            assert (
                7
                == client.get_objects(
                    "book",
                    book_keys,
                    selectors=["/meta/id"],
                    common_options=dict(fetch_performance_statistics=True),
                    enable_structured_response=True,
                )["performance_statistics"]["read_phase_count"]
            )

    def test_check_permission(self, example_env):
        user_id = example_env.client.create_object("user")
        editor_id = example_env.client.create_object(
            "editor",
            {
                "meta": {
                    "acl": [{"subjects": [user_id], "permissions": ["write"], "action": "allow"}]
                },
                "spec": {"name": "Greg"},
            },
        )

        # Sync access control.
        sync_access_control()

        # Phases: Existence + access controls.
        assert (
            2
            == example_env.client.check_object_permissions(
                [
                    dict(
                        object_type="editor",
                        object_id=editor_id,
                        subject_id=user_id,
                        permission="write",
                    )
                ],
                common_options=dict(fetch_performance_statistics=True),
                enable_structured_response=True,
            )["performance_statistics"]["read_phase_count"]
        )

    def test_update_objects(self, example_env):
        nexus_key = example_env.client.create_object("nexus", request_meta_response=True)
        mother_ship_key = example_env.client.create_object(
            "mother_ship",
            attributes=dict(meta=dict(parent_key=nexus_key)),
            request_meta_response=True,
        )

        interceptor_ids = example_env.client.create_objects(
            (
                ("interceptor", dict(meta=dict(parent_key=mother_ship_key)))
                for _ in range(20)
            ),
        )
        updates = []
        for interceptor_id in interceptor_ids:
            updates.append(
                dict(
                    object_type="interceptor",
                    object_id=dict(id=int(interceptor_id)),
                    set_updates=[
                        dict(
                            path="/spec/target_x",
                            value=356,
                        )
                    ],
                ),
            )

        # Phases: ..., watch log writer meta identity.
        assert (
            3
            == example_env.client.update_objects(
                updates,
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

        user_id = example_env.client.create_object("user")
        sync_access_control()

        example_env.client.update_object(
            "schema",
            "mother_ship",
            set_updates=[
                dict(
                    path="/meta/acl/end",
                    value=dict(
                        permissions=["write"],
                        subjects=[user_id],
                        action="allow",
                    ),
                ),
            ],
        )

        sync_access_control()

        with example_env.create_client(
            config=dict(enable_ssl=False, user=user_id),
        ) as example_user_client:
            assert (
                9
                == example_user_client.update_objects(
                    updates,
                    common_options=dict(fetch_performance_statistics=True),
                )["performance_statistics"]["read_phase_count"]
            )

    def test_update_objects_with_history_enabled(self, example_env):
        publisher_id = example_env.create_publisher()
        response = example_env.client.create_objects(
            (
                ("book", dict(meta=dict(isbn="978-1449355739", parent_key=str(publisher_id))))
                for _ in range(40)
            ),
            common_options=dict(fetch_performance_statistics=True),
            request_meta_response=True,
            enable_structured_response=True,
        )
        assert 2 == response["performance_statistics"]["read_phase_count"]

        response = example_env.client.update_objects(
            (
                dict(
                    object_type="book",
                    object_id=str(book["meta"]["key"]),
                    set_updates=[dict(path="/spec/year", value=2013)],
                )
                for book in response["subresponses"]
            ),
            common_options=dict(fetch_performance_statistics=True),
        )
        assert 4 == response["performance_statistics"]["read_phase_count"]

    def test_update_book_spec_name(self, example_env):
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher)

        # Load phases: load /spec/in_stock, load finalizers, load watchlog attributes.
        assert 3 == example_env.client.update_object(
            "book",
            str(book),
            [{"path": "/spec/in_stock", "value": 200}],
            common_options={"fetch_performance_statistics": True}
        )["performance_statistics"]["read_phase_count"]

    def test_update_one_to_many(self, example_env):
        editors = example_env.client.create_objects((("editor", dict()) for _ in range(10)))
        response = example_env.client.create_objects(
            (("publisher", dict(spec=dict(editor_in_chief=editors[i]))) for i in range(10)),
            request_meta_response=True,
            enable_structured_response=True,
            common_options=dict(fetch_performance_statistics=True),
        )
        assert 3 == response["performance_statistics"]["read_phase_count"]
        publishers = [subresponse["meta"] for subresponse in response["subresponses"]]
        response = example_env.client.create_objects(
            (("publisher", dict(spec=dict(editor_in_chief=editors[0]))) for _ in range(10)),
            request_meta_response=True,
            enable_structured_response=True,
        )
        for subresponse in response["subresponses"]:
            publishers.append(subresponse["meta"])
        assert (
            6
            == example_env.client.update_objects(
                (
                    dict(
                        object_type="publisher",
                        object_id=publisher,
                        set_updates=[
                            dict(
                                path="/spec/editor_in_chief",
                                value="",
                            )
                        ],
                    )
                    for publisher in publishers
                ),
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_update_many_to_many(self, example_env):
        author_ids = example_env.client.create_objects(
            (
                ("author", dict(spec=dict(name="Greg_{}".format(i))))
                for i in range(10)
            )
        )
        publisher = example_env.create_publisher()
        books = []
        for _ in range(10):
            books.append(example_env.create_book(publisher_id=publisher))
        pick_authors = self._pick_authors(author_ids)
        assert (
            6
            == example_env.client.update_objects(
                (
                    dict(
                        object_type="book",
                        object_id=str(book),
                        set_updates=[
                            dict(path="/spec/author_ids", value=next(pick_authors))
                        ],
                    )
                    for book in books
                ),
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_create_objects(self, example_env):
        nexus_key = example_env.client.create_object("nexus", request_meta_response=True)
        executor_ids = example_env.client.create_objects((("executor", dict()) for _ in range(10)))

        # Phases: schema + executors existence, schema + executors access control,
        #         mother ship existence, executors meta for watch log.
        assert (
            4
            == example_env.client.create_objects(
                (
                    (
                        "mother_ship",
                        dict(meta=dict(parent_key=nexus_key), spec=dict(executor_id=int(id))),
                    )
                    for id in executor_ids
                ),
                request_meta_response=True,
                enable_structured_response=True,
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_remove_object(self, example_env):
        editor_id = example_env.create_editor()
        publisher_ids = example_env.client.create_objects(
            (
                ("publisher", dict(spec=dict(editor_in_chief=editor_id)))
                for _ in range(10)
            )
        )

        assert (
            5
            == example_env.client.remove_objects(
                [("publisher", str(publisher_id)) for publisher_id in publisher_ids],
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_remove_objects_with_children(self, example_env):
        editor_id = example_env.create_editor()
        publisher_id = example_env.create_publisher(editor_id=editor_id)
        example_env.client.create_objects(
            (
                ("book", dict(meta=dict(parent_key=str(publisher_id))))
                for _ in range(10)
            )
        )

        assert (
            8
            == example_env.client.remove_object(
                "publisher",
                str(publisher_id),
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_remove_objects_hierarchy(self, example_env):
        def create_objects():
            nexus_key = example_env.client.create_object("nexus", request_meta_response=True)

            responses = example_env.client.create_objects(
                (
                    (
                        "mother_ship",
                        dict(meta=dict(parent_key=nexus_key)),
                    )
                    for i in range(15)
                ),
                request_meta_response=True,
                enable_structured_response=True,
            )["subresponses"]

            for mother_ship in responses:
                example_env.client.create_objects(
                    (
                        (
                            "interceptor",
                            dict(meta=dict(parent_key=str(mother_ship["meta"]["id"]))),
                        )
                        for _ in range(5 + (mother_ship == responses[3]) * 10)
                    ),
                    request_meta_response=True,
                    enable_structured_response=True,
                )
            return nexus_key

        assert (
            9
            == example_env.client.remove_object(
                "nexus",
                create_objects(),
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_remove_many_to_one_with_references(self, example_env):
        publisher = example_env.create_publisher()
        illustrators = example_env.client.create_objects((
            ("illustrator", dict(meta=dict(publisher_id=publisher)))
            for _ in range(10)
        ))
        publishers = example_env.client.create_objects(
            (
                ("publisher", dict(spec=dict(illustrator_in_chief=int(illustrators[i % 10]))))
                for i in range(20)
            )
        )
        removing_objects = list(zip(["illustrator"] * len(illustrators), illustrators)) + list(
            zip(["publisher"] * len(publishers), publishers)
        )

        assert (
            4
            == example_env.client.remove_objects(
                removing_objects,
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_remove_many_to_many_inline(self, example_env):
        author_ids = example_env.client.create_objects(
            (
                ("author", dict(spec=dict(name="Greg_{}".format(i))))
                for i in range(20)
            )
        )
        publisher = example_env.create_publisher()
        pick_authors = self._pick_authors(author_ids)
        books = example_env.client.create_objects(
            (
                ("book", dict(meta=dict(parent_key=str(publisher)), spec=dict(author_ids=next(pick_authors))))
                for _ in range(10)
            )
        )

        assert (
            7
            == example_env.client.remove_objects(
                [("book", str(book_id)) for book_id in books],
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_remove_many_to_many_tabular(self, example_env):
        author_ids = example_env.client.create_objects(
            (
                ("author", dict(spec=dict(name="Greg_{}".format(i))))
                for i in range(20)
            )
        )
        publisher = example_env.create_publisher()
        pick_authors = self._pick_authors(author_ids)
        example_env.client.create_objects(
            (
                ("book", dict(meta=dict(parent_key=str(publisher)), spec=dict(author_ids=next(pick_authors))))
                for _ in range(10)
            )
        )

        assert (
            6
            == example_env.client.remove_objects(
                [("author", author) for author in author_ids],
                common_options=dict(fetch_performance_statistics=True),
            )["performance_statistics"]["read_phase_count"]
        )

    def test_upsert(self, example_env):
        start_timestamp = example_env.client.generate_timestamp()
        example_config = {
            "watch_manager": {
                "changed_attributes_paths_per_type": {"author": ["/spec"]},
                "query_selector_enabled_per_type": {
                    "author": True,
                },
            }
        }
        example_env.set_cypress_config_patch(example_config)

        def does_not_raise():
            example_env.client.watch_objects(
                "author", start_timestamp=start_timestamp, selectors=["/spec"]
            )
            return True

        wait(does_not_raise, ignore_exceptions=True)

        author_ids = example_env.client.create_objects(
            (
                ("author", dict(spec=dict(name="Greg_{}".format(i))))
                for i in range(100)
            )
        )

        subrequests = [
            {
                "object_type": "author",
                "attributes": {"meta": {"id": author_id}},
                "update_if_existing": {"set_updates": [{"path": "/spec/name", "value": "John"}]},
            }
            for author_id in author_ids
        ]
        response = example_env.client.create_objects(
            subrequests,
            enable_structured_response=True,
            request_meta_response=True,
            common_options=dict(fetch_performance_statistics=True),
        )
        assert 3 == response["performance_statistics"]["read_phase_count"]

    def test_zerodiff_m2m_update(self, example_env):
        author_ids = example_env.client.create_objects(
            (
                ("author", dict(spec=dict(name="Greg_{}".format(i))))
                for i in range(20)
            )
        )
        publisher = example_env.create_publisher()
        book = example_env.create_book(publisher_id=publisher, spec=dict(author_ids=author_ids[:5]))

        start_timestamp = example_env.client.generate_timestamp()
        example_env.client.update_object(
            "book", str(book), set_updates=[dict(path="/spec/author_ids", value=author_ids[1:6])]
        )
        timestamp = example_env.client.generate_timestamp()
        events = example_env.client.watch_objects(
            "author",
            start_timestamp=start_timestamp,
            timestamp=timestamp,
            time_limit=timedelta(seconds=10),
        )
        assert 2 == len(events)

    def test_watches(self, example_env):
        config = dict(watch_manager=dict(query_filter_enabled=True, labels_store_enabled=True))
        example_env.set_cypress_config_patch(config)

        author1 = example_env.create_author("Greg", labels=dict(enabled=True))
        author2 = example_env.create_author("Stan")
        start_timestamp = example_env.client.generate_timestamp()
        timestamp = example_env.client.generate_timestamp()
        continuation_token = example_env.client.watch_objects(
            "author",
            start_timestamp=start_timestamp,
            filter="[/labels/enabled] = true",
            enable_structured_response=True,
        )["continuation_token"]

        example_env.client.update_objects(
            (
                dict(
                    object_type="author",
                    object_identity=author1 if i % 50 == 0 else author2,
                    set_updates=[dict(path="/spec/age", value=i)],
                )
                for i in range(1001)
            ),
            common_options=dict(fetch_performance_statistics=True),
        )["performance_statistics"]["read_phase_count"]

        timestamp = example_env.client.generate_timestamp()

        read_phase_count = example_env.client.watch_objects(
            "author",
            continuation_token=continuation_token,
            timestamp=timestamp,
            time_limit=timedelta(seconds=5),
            event_count_limit=20,
            enable_structured_response=True,
            filter="[/labels/enabled] = true",
            common_options=dict(fetch_performance_statistics=True),
        )["performance_statistics"]["read_phase_count"]
        assert read_phase_count <= 7

    @pytest.mark.parametrize("object_count", [1, 20, 50])
    def test_revision(self, object_count, example_env):
        nexus_key = example_env.client.create_object("nexus", request_meta_response=True)
        executor_ids = example_env.client.create_objects((("executor", dict()) for _ in range(object_count)))
        mother_ship_ids = example_env.client.create_objects(
            (
                (
                    "mother_ship",
                    dict(meta=dict(parent_key=nexus_key), spec=dict(executor_id=int(id))),
                )
                for id in executor_ids
            )
        )

        updates = []
        for mother_ship_id in mother_ship_ids:
            updates.append(
                dict(
                    object_type="mother_ship",
                    object_id=dict(id=int(mother_ship_id)),
                    set_updates=[dict(path="/meta/release_year", value=1777)],
                ),
            )

        read_phase_count = example_env.client.update_objects(
            updates,
            common_options=dict(fetch_performance_statistics=True),
        )["performance_statistics"]["read_phase_count"]

        assert read_phase_count == 3
