from yt.wrapper.errors import YtTabletTransactionLockConflict

from yt.yson.yson_types import YsonEntity

from contextlib import nullcontext as does_not_raise

import copy
import pytest

from pytest_lazy_fixtures import lf


class BaseTestUpdateCompositeAttribute:
    @pytest.fixture
    def object_count(self):
        raise NotImplementedError

    @pytest.fixture
    def update_objects(self):
        raise NotImplementedError

    @pytest.fixture
    def parent_key(self, example_env):
        return example_env.client.create_object(
            "publisher",
            {"spec": {"name": "O'REILLY"}},
            request_meta_response=True,
        )

    @pytest.fixture
    def empty_book_spec(self, parent_key, example_env):
        book_id = example_env.client.create_object(
            "book",
            {"meta": {"isbn": "978-1449355739", "parent_key": parent_key}},
            request_meta_response=True,
        )
        return example_env.client.get_object("book", book_id, selectors=["/spec"])[0]

    @pytest.fixture
    def object_keys(self, example_env, parent_key, object_count):
        attributes = {
            "spec": {
                "name": "Learning python",
                "year": 2021,
                "font": "Times New Roman",
                "genres": ["first_genre", "second_genre"],
                "keywords": ["programming", "python"],
                "design": {
                    "color": "red",
                },
                "page_count": 42,
                "editor_id": example_env.client.create_object("editor", {"spec": {"name": "Greg"}}),
                "digital_data": {
                    "store_rating": 4.9,
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
                "chapter_descriptions": [
                    {"name": "I"},
                    {"name": "II"},
                ],
            },
            "meta": {
                "isbn": "978-1449355739",
                "parent_key": parent_key,
            },
        }
        return example_env.client.create_objects((("book", attributes) for _ in range(object_count)))

    @pytest.fixture
    def get_objects(self, example_env, object_keys):
        def _inner(selectors=["/meta", "/spec", "/status", "/annotations"]):
            get_result = example_env.client.get_objects("book", object_keys, selectors=selectors)
            assert len(get_result) == len(object_keys)
            for subresult in get_result:
                assert len(subresult) == len(selectors)
            return get_result

        return _inner

    @pytest.fixture
    def expected_spec(self, empty_book_spec, value):
        spec = copy.deepcopy(empty_book_spec)
        spec.update(value)
        return spec

    @pytest.mark.parametrize(
        "path,value",
        [
            pytest.param("/spec", {}, id="set_empty_message"),
            pytest.param(
                "/spec",
                {
                    "name": "Learning Python 3",
                },
                id="set_etc_primitive",
            ),
            pytest.param(
                "/spec",
                {
                    "design": {
                        "color": "white",
                    },
                },
                id="set_etc_message",
            ),
            pytest.param(
                "/spec",
                {
                    "font": "Comic Sans",
                },
                id="set_column_primitive",
            ),
            pytest.param(
                "/spec",
                {
                    "digital_data": {
                        "store_rating": 5.0,
                        "available_formats": [
                            {
                                "format": 1,
                                "size": 12778383,
                            },
                        ],
                    }
                },
                id="set_column_message",
            ),
            pytest.param(
                "/spec",
                {
                    "name": "Learning Python 3",
                    "font": "Comic Sans",
                    "genres": ["Programming"],
                    "design": {
                        "illustrations": ["first_image", "second_image"],
                    },
                },
                id="set_different",
            ),
            pytest.param(
                "/spec",
                {
                    "chapter_descriptions": [
                        {"name": "I"},
                        {"name": "II"},
                        {"name": "III"},
                    ],
                },
                id="repeated_column_attribute",
            ),
        ],
    )
    def test_update_path(self, path, value, expected_spec, update_objects, get_objects):
        expected = get_objects()
        for expected_object in expected:
            expected_object[1] = expected_spec
            if "api_revision" in expected_object[1]:
                del expected_object[1]["api_revision"]

        update_objects(
            [
                {
                    "path": path,
                    "value": value,
                },
            ]
        )
        with does_not_raise():
            actual = get_objects()
            for actual_object in actual:
                del actual_object[1]["api_revision"]
            assert expected == actual

    @pytest.mark.parametrize(
        "remove_path,get_path,result",
        [
            pytest.param(
                "/spec/digital_data",
                "/spec/digital_data",
                dict(),
                id="composite_map_attribute",
            ),
            pytest.param(
                "/spec/chapter_descriptions",
                "/spec/chapter_descriptions",
                list(),
                id="composite_list_attribute",
            ),
            pytest.param(
                "/spec/design",
                "/spec/design",
                YsonEntity(),
                id="composite_map_attribute_from_etc",
            ),
            pytest.param(
                "/spec/year",
                "/spec/year",
                0,
                id="primitive_scalar_separate_column_attribute",
            ),
            pytest.param(
                "/spec/page_count",
                "/spec/page_count",
                None,
                id="primitive_scalar_attribute",
            ),
            pytest.param(
                "/spec",
                "/spec",
                lf("empty_book_spec"),
                id="composite_nonscalar_attribute",
            ),
        ],
    )
    def test_remove_path(self, remove_path, get_path, result, update_objects, get_objects):
        update_objects([], remove_updates=[{"path": remove_path}])
        with does_not_raise():
            actual = get_objects([get_path])
            for object_result in actual:
                if get_path == "/spec":
                    if "api_revision" in object_result[0]:
                        del object_result[0]["api_revision"]
                    if "api_revision" in result:
                        del result["api_revision"]
                assert object_result[0] == result


class TestUpdateSingleObjectCompositeAttribute(BaseTestUpdateCompositeAttribute):
    @pytest.fixture
    def object_count(self):
        return 1

    @pytest.fixture
    def update_objects(self, example_env, object_keys):
        assert 1 == len(object_keys)

        def _inner(set_updates, remove_updates=None, lock_updates=None):
            return example_env.client.update_object("book", object_keys[0], set_updates, remove_updates, lock_updates)

        return _inner

    def test_append(self, update_objects, get_objects):
        update_objects(set_updates=[dict(path="/spec/chapter_descriptions/end", value=dict(name="Last"))])
        assert get_objects(["/spec/chapter_descriptions"])[0][0][-1] == dict(name="Last")
        update_objects(set_updates=[dict(path="/spec/design/illustrations/end", value="last")])
        assert get_objects(["/spec/design/illustrations"])[0][0][-1] == "last"


class TestUpdateBatchObjectsCompositeAttribute(BaseTestUpdateCompositeAttribute):
    @pytest.fixture
    def object_count(self):
        return 30

    @pytest.fixture
    def update_objects(self, example_env, object_keys):
        assert 1 < len(object_keys)

        def _inner(set_updates, remove_updates=None):
            subrequests = [
                {
                    "object_type": "book",
                    "object_id": id,
                    "set_updates": set_updates,
                    "remove_updates": remove_updates,
                }
                for id in object_keys
            ]
            result = example_env.client.update_objects(subrequests, common_options=dict(fetch_performance_statistics=True))
            assert result["performance_statistics"]["read_phase_count"] < len(object_keys)
            return result

        return _inner


class TestLockComposite(object):
    def test_lock_spec(self, example_env):
        publisher = example_env.create_publisher()
        authors = [example_env.create_author(name="Greg_{}".format(i)) for i in range(10)]
        book = example_env.create_book(publisher_id=publisher, spec=dict(author_ids=authors))
        client = example_env.client

        tx1_id = client.start_transaction()
        tx2_id = client.start_transaction()
        tx3_id = client.start_transaction()
        tx4_id = client.start_transaction()
        tx5_id = client.start_transaction()

        client.update_object(
            "book",
            str(book),
            lock_updates=[{"path": "/spec", "lock_type": "shared_strong"}],
            transaction_id=tx1_id,
        )
        client.update_object(
            "book",
            str(book),
            lock_updates=[{"path": "/spec", "lock_type": "shared_strong"}],
        )
        client.get_object("book", str(book), selectors=["/spec"])

        client.remove_object("book", str(book), transaction_id=tx2_id)

        client.update_object(
            "book",
            str(book),
            set_updates=[{"path": "/spec/name", "value": "Learning Python"}],
            transaction_id=tx3_id,
        )

        client.update_object(
            "book",
            str(book),
            set_updates=[{"path": "/spec/author_ids", "value": []}],
            transaction_id=tx4_id,
        )

        client.update_object(
            "book",
            str(book),
            set_updates=[{"path": "/status/released", "value": True}],
            transaction_id=tx5_id,
        )

        client.commit_transaction(tx1_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            client.commit_transaction(tx2_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            client.commit_transaction(tx3_id)
        with pytest.raises(YtTabletTransactionLockConflict):
            client.commit_transaction(tx4_id)
        client.commit_transaction(tx5_id)
