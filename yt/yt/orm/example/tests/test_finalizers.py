from .conftest import sync_access_control

from yt.orm.library.common import (
    AuthorizationError,
    InvalidObjectStateError,
    InvalidRequestArgumentsError,
    NoSuchObjectError,
)

from yt.common import YtResponseError

import datetime
import pytest


@pytest.fixture
def create_publisher_with_finalizer(example_env):
    def create(**finalizers):
        return example_env.client.create_object("publisher", {"meta": {"finalizers": finalizers}})

    return create


@pytest.fixture
def publisher_with_finalizer(create_publisher_with_finalizer):
    return create_publisher_with_finalizer(finalizer={})


class TestFinalizers:
    EXAMPLE_MASTER_CONFIG = {
        "transaction_manager": {
            "allow_parent_removal_override": True,
        },
    }

    def test_user_api(self, example_env, regular_user1, publisher_with_finalizer):
        example_env.client.update_object(
            "publisher",
            publisher_with_finalizer,
            set_updates=[
                {
                    "path": "/meta/acl/end",
                    "value": {
                        "subjects": [regular_user1.id],
                        "permissions": ["write"],
                        "action": "allow",
                    },
                }
            ],
        )
        sync_access_control()

        with pytest.raises(AuthorizationError):
            regular_user1.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[{"path": "/meta/finalizers/end", "value": {"fin": {}}}],
            )
        with pytest.raises(InvalidRequestArgumentsError):
            regular_user1.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[
                    {"path": "/control/add_finalizer", "value": dict(finalizer_name="finalizer")}
                ],
            )

        regular_user1.client.update_object(
            "publisher",
            publisher_with_finalizer,
            set_updates=[{"path": "/control/add_finalizer", "value": dict(finalizer_name="fin")}],
        )
        regular_user1.client.remove_object("publisher", publisher_with_finalizer)
        regular_user1.client.update_object(
            "publisher",
            publisher_with_finalizer,
            set_updates=[
                {"path": "/control/complete_finalization", "value": dict(finalizer_name="fin")}
            ],
        )
        with pytest.raises(InvalidRequestArgumentsError):
            regular_user1.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[
                    {"path": "/control/complete_finalization", "value": dict(finalizer_name="fin")}
                ],
            )
        with pytest.raises(InvalidRequestArgumentsError):
            regular_user1.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[
                    {
                        "path": "/control/complete_finalization",
                        "value": dict(finalizer_name="nonexistent"),
                    }
                ],
            )

        with pytest.raises(AuthorizationError):
            regular_user1.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[
                    {
                        "path": "/control/complete_finalization",
                        "value": dict(finalizer_name="finalizer"),
                    }
                ],
            )

        finalizers = example_env.client.get_object(
            "publisher", publisher_with_finalizer, ["/meta/active_finalizers"]
        )[0]
        assert finalizers.keys() == {"finalizer"}

    def test_finalization(self, example_env, publisher_with_finalizer):
        ts = example_env.client.generate_timestamp()
        now = ts / 2 ** 30
        response = example_env.client.remove_object("publisher", publisher_with_finalizer)
        finalization_start_time = response["finalization_start_time"]
        assert now < finalization_start_time
        response = example_env.client.remove_object("publisher", publisher_with_finalizer)
        assert response["finalization_start_time"] == finalization_start_time

        example_env.client.update_object(
            "publisher", publisher_with_finalizer,
            set_updates=[dict(path="/control/complete_finalization", value=dict(finalizer_name="finalizer"))]
        )
        end_ts = example_env.client.generate_timestamp()

        events = example_env.client.watch_objects(
            "publisher",
            start_timestamp=ts,
            timestamp=end_ts,
            time_limit=datetime.timedelta(seconds=30),
        )
        assert len(events) == 2
        assert events[0]["event_type"] == "object_updated"
        assert events[1]["event_type"] == "object_removed"

        events = example_env.client.select_object_history(
            "publisher",
            publisher_with_finalizer,
            ["/meta/finalization_start_time"],
            options=dict(timestamp_interval=[ts, None]),
        )["events"]
        assert len(events) == 2
        assert events[0]["event_type"] == 3
        assert events[1]["event_type"] == 2

        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object(
                "publisher", publisher_with_finalizer, selectors=["/meta/key"]
            )

    def test_select_finalizing_objects(self, example_env, publisher_with_finalizer):
        client = example_env.client
        client.remove_object("publisher", publisher_with_finalizer)

        result = []

        def contains_publisher():
            return any(map(lambda item: publisher_with_finalizer == item[0], result))

        result = client.select_objects("publisher", selectors=["/meta/key"])
        assert contains_publisher()
        result = client.select_objects(
            "publisher", selectors=["/meta/key"], options={"fetch_finalizing_objects": False}
        )
        assert not contains_publisher()

        example_config = {
            "transaction_manager": {
                "fetch_finalizing_objects_by_default": False,
            }
        }
        with example_env.set_cypress_config_patch_in_context(example_config):
            result = client.select_objects("publisher", selectors=["/meta/key"])
            assert not contains_publisher()
            result = client.select_objects(
                "publisher", selectors=["/meta/key"], options={"fetch_finalizing_objects": True}
            )
            assert contains_publisher()

    def test_create_after_finalize(self, example_env, publisher_with_finalizer):
        client = example_env.client

        tx = client.start_transaction()
        client.remove_object("publisher", publisher_with_finalizer, transaction_id=tx)
        with pytest.raises(YtResponseError):
            client.create_object(
                "publisher", {"meta": {"key": publisher_with_finalizer}}, transaction_id=tx
            )

        client.remove_object("publisher", publisher_with_finalizer)
        with pytest.raises(YtResponseError):
            example_env.client.create_object(
                "publisher", {"meta": {"key": publisher_with_finalizer}}
            )

    def test_finalization_with_references(self, example_env, publisher_with_finalizer):
        publisher = example_env.client.create_object("publisher")
        editor = example_env.client.create_object("editor")
        example_env.client.update_object(
            "publisher",
            publisher_with_finalizer,
            set_updates=[
                {"path": "/spec/publisher_group", "value": int(publisher)},
                {"path": "/spec/editor_in_chief", "value": editor},
            ],
        )

        # /spec/publisher_group forbids removal (and so, finalization) with nonempty reference.
        with pytest.raises(YtResponseError):
            example_env.client.remove_object("publisher", publisher_with_finalizer)

        example_env.client.update_object(
            "publisher",
            publisher_with_finalizer,
            remove_updates=[{"path": "/spec/publisher_group"}],
        )

        response = example_env.client.remove_object("publisher", publisher_with_finalizer)
        assert response.get("finalization_start_time") is not None

        assert not list(
            example_env.yt_client.select_rows(
                " * from [//home/example/db/editor_to_publishers] limit 1"
            )
        )

    def test_direct_update_forbidden(self, example_env, publisher_with_finalizer):
        example_env.client.remove_object("publisher", publisher_with_finalizer)

        with pytest.raises(InvalidObjectStateError):
            example_env.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[{"path": "/status/licensed", "value": True}],
            )
        illustrator = example_env.create_illustrator(name="Greg")
        with pytest.raises(InvalidObjectStateError):
            example_env.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[{"path": "/spec/illustrator_in_chief", "value": illustrator}],
            )

        with pytest.raises(InvalidObjectStateError):
            example_env.client.update_object(
                "publisher",
                publisher_with_finalizer,
                set_updates=[{"path": "/meta/finalizers/end", "value": dict()}],
            )

    def test_referencing_forbidden(self, example_env, publisher_with_finalizer):
        example_env.client.remove_object("publisher", publisher_with_finalizer)
        pub = example_env.client.create_object("publisher")
        book = example_env.create_book()

        finalizing_author = example_env.client.create_object(
            "author", dict(meta=dict(finalizers=dict(finalizer={})), spec=dict(name="Greg"))
        )
        example_env.client.remove_object("author", finalizing_author)

        # Add `many` to finalizing `one` part.
        with pytest.raises(InvalidObjectStateError):
            example_env.client.create_object(
                "illustrator",
                dict(meta=dict(publisher_id=int(pub), part_time_job=int(publisher_with_finalizer))),
            )

        # Add child to finalizing parent.
        with pytest.raises(InvalidObjectStateError):
            example_env.client.create_object(
                "illustrator", dict(meta=dict(publisher_id=int(publisher_with_finalizer)))
            )

        # Add inline `many` to finalizing tabular `many` part.
        with pytest.raises(InvalidObjectStateError):
            example_env.client.update_object(
                "book",
                book,
                set_updates=[
                    dict(
                        path="/spec/alternative_publisher_ids",
                        value=[int(publisher_with_finalizer)],
                    )
                ],
            )

        # Add `many` via new references.
        with pytest.raises(InvalidObjectStateError):
            example_env.client.update_object(
                "book", book, set_updates=[dict(path="/spec/author_ids", value=[finalizing_author])]
            )

    @staticmethod
    def _create_child(client, publisher_with_finalizer):
        tx = client.start_transaction()
        client.create_object(
            "illustrator",
            dict(meta=dict(publisher_id=int(publisher_with_finalizer))),
            transaction_id=tx,
        )
        return tx

    @staticmethod
    def _add_finalizer(client, publisher_with_finalizer):
        tx = client.start_transaction()
        client.update_object(
            "publisher",
            publisher_with_finalizer,
            set_updates=[{"path": "/control/add_finalizer", "value": dict(finalizer_name="fin")}],
            transaction_id=tx,
        )
        return tx

    @pytest.mark.parametrize("commit_first", [pytest.param("finalization"), pytest.param("action")])
    @pytest.mark.parametrize("action", [_create_child, _add_finalizer])
    def test_concurrent_child_addition(
        self, example_env, publisher_with_finalizer, commit_first, action
    ):
        client = example_env.example_client

        tx1 = client.start_transaction()
        client.remove_object("publisher", publisher_with_finalizer, transaction_id=tx1)

        tx2 = action(client, publisher_with_finalizer)

        first, second = (tx1, tx2) if commit_first == "finalization" else (tx2, tx1)
        client.commit_transaction(first)
        with pytest.raises(YtResponseError):
            client.commit_transaction(second)

    def test_concurrent_children_finalization(self, example_env, publisher_with_finalizer):
        client = example_env.example_client

        illustrator1 = client.create_object(
            "illustrator", dict(meta=dict(publisher_id=int(publisher_with_finalizer), finalizers=dict(f1={})))
        )
        illustrator2 = client.create_object(
            "illustrator", dict(meta=dict(publisher_id=int(publisher_with_finalizer), finalizers=dict(f1={})))
        )
        client.remove_object("publisher", publisher_with_finalizer)

        tx1 = client.start_transaction()
        tx2 = client.start_transaction()
        client.update_object(
            "illustrator",
            illustrator1,
            set_updates=[{"path": "/control/complete_finalization", "value": dict(finalizer_name="f1")}],
            transaction_id=tx1,
        )
        client.update_object(
            "illustrator",
            illustrator2,
            set_updates=[{"path": "/control/complete_finalization", "value": dict(finalizer_name="f1")}],
            transaction_id=tx2,
        )

        client.commit_transaction(tx1)
        with pytest.raises(YtResponseError):
            client.commit_transaction(tx2)

    def test_create_remove(self, example_env):
        tx = example_env.client.start_transaction()
        obj = example_env.client.create_object(
            "publisher", {"meta": {"finalizers": {"fin": {}}}}, transaction_id=tx
        )
        example_env.client.remove_object("publisher", obj, transaction_id=tx)
        example_env.client.commit_transaction(tx)

        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("publisher", obj, ["/meta/id"])

    def test_parent_finalization(self, example_env, publisher_with_finalizer):
        book = example_env.create_book(publisher_id=publisher_with_finalizer)
        example_env.client.remove_object("publisher", publisher_with_finalizer)

        result = example_env.client.get_object(
            "book", book, selectors=["/meta/finalizers", "/meta/finalization_start_time"]
        )
        assert set(result[0].keys()) == {"_orm_parent"}
        assert bool(result[1])

        example_env.client.update_object(
            "publisher",
            publisher_with_finalizer,
            remove_updates=[{"path": "/meta/finalizers/finalizer"}],
        )
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("book", book, selectors=["/meta/key"])
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object(
                "publisher", publisher_with_finalizer, selectors=["/meta/key"]
            )

    def test_hierarchy_removal_with_finalizing_child(self, example_env):
        nexus = example_env.client.create_object("nexus")

        mother_ship1 = example_env.client.create_object(
            "mother_ship",
            attributes=dict(meta=dict(parent_key=nexus, finalizers=dict(finalizer={}))),
        )
        interceptor1 = example_env.client.create_objects(
            ("interceptor", {"meta": {"mother_ship_id": int(mother_ship1)}}) for i in range(15)
        )[0]

        mother_ship2 = example_env.client.create_objects(
            ("mother_ship", dict(meta=dict(parent_key=nexus))) for i in range(15)
        )[0]
        interceptor2 = example_env.client.create_objects(
            ("interceptor", {"meta": {"mother_ship_id": int(mother_ship2)}}) for i in range(15)
        )[0]

        mother_ship3 = example_env.client.create_object(
            "mother_ship",
            attributes=dict(meta=dict(parent_key=nexus, finalizers=dict(finalizer={}))),
        )
        interceptor3 = example_env.client.create_objects(
            ("interceptor", {"meta": {"mother_ship_id": int(mother_ship3)}}) for i in range(15)
        )[0]

        example_env.client.remove_object("nexus", nexus)

        for object_type, key, finalizer_key in (
            ("nexus", nexus, "_orm_children"),
            ("mother_ship", mother_ship1, "finalizer"),
            ("mother_ship", mother_ship2, "_orm_parent"),
            ("mother_ship", mother_ship3, "finalizer"),
            ("interceptor", interceptor1, "_orm_parent"),
            ("interceptor", interceptor2, "_orm_parent"),
            ("interceptor", interceptor3, "_orm_parent"),
        ):
            result = example_env.client.get_object(
                object_type, key, selectors=["/meta/finalizers"]
            )[0]
            assert set(result.keys()) == {
                finalizer_key
            }, f"Invalid finalizers {result.keys()} for {object_type} {key}"

        # TODO(dgolear): Optimize read phaase count for hierarchy removals.
        patch = {"transaction_manager": {"read_phase_hard_limit": 17}}
        with example_env.set_cypress_config_patch_in_context(patch):
            example_env.client.update_object(
                "mother_ship", mother_ship1, remove_updates=[{"path": "/meta/finalizers/finalizer"}]
            )
        example_env.client.get_object("nexus", nexus, selectors=["/meta/key"])
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("mother_ship", mother_ship1, selectors=["/meta/key"])
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("interceptor", interceptor1, selectors=["/meta/key"])

        example_env.client.get_object("mother_ship", mother_ship2, selectors=["/meta/key"])
        example_env.client.get_object("mother_ship", mother_ship3, selectors=["/meta/key"])
        example_env.client.get_object("interceptor", interceptor2, selectors=["/meta/key"])
        example_env.client.get_object("interceptor", interceptor3, selectors=["/meta/key"])

        with example_env.set_cypress_config_patch_in_context(patch):
            example_env.client.update_object(
                "mother_ship", mother_ship3, remove_updates=[{"path": "/meta/finalizers/finalizer"}]
            )
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("nexus", nexus, selectors=["/meta/key"])
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("mother_ship", mother_ship2, selectors=["/meta/key"])
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("mother_ship", mother_ship3, selectors=["/meta/key"])
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("interceptor", interceptor2, selectors=["/meta/key"])
        with pytest.raises(NoSuchObjectError):
            example_env.client.get_object("interceptor", interceptor3, selectors=["/meta/key"])

    def test_remove_finalizer(self, example_env, create_publisher_with_finalizer):
        publisher = create_publisher_with_finalizer(finalizer1={}, finalizer2={})

        example_env.client.update_object(
            "publisher",
            publisher,
            set_updates=[
                {"path": "/control/remove_finalizer", "value": dict(finalizer_name="finalizer1")}
            ],
        )

        finalizers = example_env.client.get_object(
            "publisher", publisher, selectors=["/meta/finalizers"]
        )[0]
        assert "finalizer1" not in finalizers

        example_env.client.remove_object("publisher", publisher)
        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "publisher",
                publisher,
                set_updates=[
                    {"path": "/control/remove_finalizer", "value": dict(finalizer_name="finalizer2")}
                ],
            )
