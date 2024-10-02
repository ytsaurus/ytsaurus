from yt.orm.library.common import InvalidRequestArgumentsError

from contextlib import nullcontext as does_not_raise

import pytest


class TestSharedWriteLock:
    @pytest.fixture
    def get_object_type_field(self, example_env):
        def _inner(object_type, selector):
            def _inner_inner(id):
                get_result = example_env.client.get_object(object_type, id, selectors=[selector])
                assert len(get_result) == 1
                return get_result[0]
            return _inner_inner
        return _inner

    @pytest.fixture
    def get_cat_name(self, get_object_type_field):
        return get_object_type_field("cat", "/spec/name")

    @pytest.fixture
    def get_cat_mood(self, get_object_type_field):
        return get_object_type_field("cat", "/spec/mood")

    def test_overlapping_scalar_attribute_updates(self, example_env, get_cat_mood):
        id = example_env.create_cat(spec={"mood": 1})

        tx1 = example_env.client.start_transaction()
        tx2 = example_env.client.start_transaction()

        example_env.client.update_object(
            "cat",
            id,
            set_updates=[{"path": "/spec/mood", "value": 3, "shared_write": True}],
            transaction_id=tx1,
        )
        example_env.client.update_object(
            "cat",
            id,
            set_root_updates=[{"paths": ["/spec/mood"], "value": {"spec" : {"mood" : 4}}, "shared_write": True}],
            transaction_id=tx2,
        )

        assert get_cat_mood(id) == 1
        example_env.client.commit_transaction(tx1)
        assert get_cat_mood(id) == 3
        example_env.client.commit_transaction(tx2)
        assert get_cat_mood(id) == 4

    def test_create_with_update_if_existing(self, example_env, get_cat_mood):
        id = example_env.create_cat(spec={"mood": 1})

        update_tx = example_env.client.start_transaction()
        create_with_update_tx = example_env.client.start_transaction()

        example_env.client.update_object(
            "cat",
            id,
            set_updates=[{"path": "/spec/mood", "value": 2, "shared_write": True}],
            transaction_id=update_tx,
        )
        example_env.client.create_object(
            "cat",
            {"meta": {"id": int(id)}},
            update_if_existing={
                "set_updates": [{"path": "/spec/mood", "value": 3, "shared_write": True}],
            },
            transaction_id=create_with_update_tx,
        )

        assert get_cat_mood(id) == 1
        example_env.client.commit_transaction(update_tx)
        assert get_cat_mood(id) == 2
        example_env.client.commit_transaction(create_with_update_tx)
        assert get_cat_mood(id) == 3

    def test_composite_update(self, example_env):
        id = example_env.client.create_object(
            "nested_columns",
            attributes={"spec": {"composite_singular": {
                "direct_repeated": [{"foo": "csdr0"}, {"foo": "csdr1"}],
                "column_map": {"cscmk": {"foo": "cscmv"}},
            }}},
        )

        tx1 = example_env.client.start_transaction()
        with pytest.raises(InvalidRequestArgumentsError, match="Composite attribute.*cannot be updated with shared write lock"):
            example_env.client.update_object(
                "nested_columns",
                id,
                set_updates=[{"path": "/spec/composite_singular", "value": {}, "shared_write": True}],
                transaction_id=tx1,
            )

    def test_etc_update(self, example_env):
        id = example_env.create_cat(spec={"mood_in_previous_days": ["playful", "sleepy"]})

        tx1 = example_env.client.start_transaction()
        with pytest.raises(InvalidRequestArgumentsError, match="Etc attribute.*cannot be updated with shared write lock"):
            example_env.client.update_object(
                "cat",
                id,
                set_updates=[{"path": "/spec/mood_in_previous_days", "value": [], "shared_write": True}],
                transaction_id=tx1,
            )

    def test_index_or_predicate_update(self, example_env):
        id = example_env.create_book(spec=dict(year=1998, font="Arial"))

        tx1 = example_env.client.start_transaction()
        with pytest.raises(
            InvalidRequestArgumentsError,
            match=".*indexed/predicate attribute .*cannot be updated with shared write lock"
        ):
            example_env.client.update_object(
                "book",
                id,
                set_updates=[{"path": "/spec/year", "value": 1997, "shared_write": True}],
                transaction_id=tx1
            )
        tx2 = example_env.client.start_transaction()
        with pytest.raises(
            InvalidRequestArgumentsError,
            match=".*indexed/predicate attribute .*cannot be updated with shared write lock"
        ):
            example_env.client.update_object(
                "book",
                id,
                set_updates=[{"path": "/spec/font", "value": "Serif", "shared_write": True}],
                transaction_id=tx2
            )

    @pytest.mark.parametrize(
        "set_update, expected_raise",
        [
            pytest.param(
                {"path": "/spec/column_singular", "value": {"foo": "foo"}, "shared_write": True},
                does_not_raise(),
                id="Update columnar message",
            ),
            pytest.param(
                {"path": "/spec/column_singular/foo", "value": "foo", "shared_write": True},
                pytest.raises(
                    InvalidRequestArgumentsError,
                    match="Attribute.*cannot be updated with shared write lock and nonempty suffix path.*"
                ),
                id="Update field in columnar message",
            ),
            pytest.param(
                {"path": "/spec/column_repeated", "value": [{"foo": "foo"}], "shared_write": True},
                does_not_raise(),
                id="Update columnar vector",
            ),
            pytest.param(
                {"path": "/spec/column_repeated/0", "value": {"foo": "foo"}, "shared_write": True},
                pytest.raises(
                    InvalidRequestArgumentsError,
                    match="Attribute.*cannot be updated with shared write lock and nonempty suffix path.*"
                ),
                id="Update element in columnar vector",
            ),
            pytest.param(
                {"path": "/spec/column_map", "value": {"key": {"foo": "foo"}}, "shared_write": True},
                does_not_raise(),
                id="Update columnar map",
            ),
            pytest.param(
                {"path": "/spec/column_map/key", "value": {"foo": "foo"}, "shared_write": True},
                pytest.raises(
                    InvalidRequestArgumentsError,
                    match="Attribute.*cannot be updated with shared write lock and nonempty suffix path.*"
                ),
                id="Update element in columnar map",
            ),
        ]
    )
    def test_update_by_path(self, example_env, set_update, expected_raise):
        id = example_env.client.create_object("nested_columns")

        tx1 = example_env.client.start_transaction()
        with expected_raise:
            example_env.client.update_object(
                "nested_columns",
                id,
                set_updates=[set_update],
                transaction_id=tx1
            )

    @pytest.mark.parametrize(
        "reference_path, expected_error_message",
        [
            pytest.param(
                "/spec/author_ids",
                "Reference field cannot be updated with shared write lock",
                id="Change old reference attribute",
            ),
            pytest.param(
                "/spec/alternative_publisher_ids",
                "Many to Many inline attribute cannot be updated with shared write lock / aggregate mode",
                id="Change new reference attribute",
            ),
        ]
    )
    def test_change_reference_attribute(self, example_env, reference_path, expected_error_message):
        book = example_env.create_book(
            publisher_id=example_env.create_publisher(),
            spec=dict(
                author_ids=[example_env.create_author()],
                alternative_publisher_ids=[example_env.create_publisher()]
            )
        )
        client = example_env.client

        tx1 = client.start_transaction()
        with pytest.raises(InvalidRequestArgumentsError, match=expected_error_message):
            client.update_object(
                "book",
                str(book),
                set_updates=[{"path": reference_path, "value": [], "shared_write": True}],
                transaction_id=tx1,
            )
