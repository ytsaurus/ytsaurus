from yt.orm.library.common import NoSuchObjectError
from yt.common import YtResponseError

import pytest


# NB! Some of those test cannot be properly torn down, since proper object removal
# is not possible. Thus, each test is run in a separate test suite.
# TODO(deep): Fix references and move all tests back to a single test suite.
class TestProtoReferenceBase:
    @pytest.fixture
    def guide_key(self, example_env):
        return example_env.create_book(book_id="42;42")

    @pytest.fixture
    def encyclopedia_key(self, example_env):
        return example_env.create_book(book_id="100;500")

    @pytest.fixture
    def vogon_poetry_key(self, example_env):
        return example_env.create_book(book_id="14;15")

    def book_ids(self, example_env, book_key):
        ids = example_env.client.get_object("book", book_key, ["/meta/id", "/meta/id2"])
        return {"id": ids[0], "id2": ids[1]}

    def assert_hitchhiker_ids(self, example_env, book_key, hitchhiker_ids):
        assert (
            hitchhiker_ids
            == example_env.client.get_object("book", book_key, ["/status/hitchhiker_ids"])[0]
        )

    def assert_no_hitchhikers(self, example_env, book_key):
        assert not example_env.client.get_object("book", book_key, ["/status/hitchhiker_ids"])[0]

    @pytest.fixture
    def arthur(self, example_env, guide_key):
        return self.create_hitchhiker(example_env, 42, self.book_ids(example_env, guide_key), None)

    @pytest.fixture
    def ford(self, example_env, guide_key, encyclopedia_key, vogon_poetry_key):
        return self.create_hitchhiker(
            example_env,
            4242,
            self.book_ids(example_env, guide_key),
            [
                self.book_ids(example_env, encyclopedia_key),
                self.book_ids(example_env, vogon_poetry_key),
            ],
        )

    @pytest.fixture
    def jeltz(self, example_env, guide_key, encyclopedia_key):
        return self.create_hitchhiker(
            example_env,
            43,
            None,
            [
                self.book_ids(example_env, encyclopedia_key),
                self.book_ids(example_env, guide_key),
            ],
        )

    @pytest.fixture
    def zaphod(self, example_env, guide_key, encyclopedia_key, vogon_poetry_key):
        return self.create_hitchhiker(example_env, 1, None, None)

    def create_hitchhiker(self, example_env, id, favorite_book_ids, hated_books_ids):
        meta = {"id": id}
        spec = {}
        if favorite_book_ids:
            spec["favorite_book"] = favorite_book_ids
        if hated_books_ids:
            spec["hated_books"] = hated_books_ids

        return example_env.client.create_object(
            "hitchhiker",
            {"meta": meta, "spec": spec},
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["id"]


class TestCreateSingle(TestProtoReferenceBase):
    def test_create_single(self, example_env, guide_key, arthur):
        self.assert_hitchhiker_ids(example_env, guide_key, [arthur])


class TestCreateSeveral(TestProtoReferenceBase):
    def test_create_several(
        self,
        example_env,
        arthur,
        ford,
        jeltz,
        zaphod,
        guide_key,
        encyclopedia_key,
        vogon_poetry_key,
    ):
        self.assert_hitchhiker_ids(example_env, guide_key, [arthur, ford, jeltz])
        self.assert_hitchhiker_ids(example_env, encyclopedia_key, [ford, jeltz])
        self.assert_hitchhiker_ids(example_env, vogon_poetry_key, [ford])


class TestCreateEmpty(TestProtoReferenceBase):
    def test_create_empty(self, example_env, zaphod, guide_key, encyclopedia_key, vogon_poetry_key):
        self.assert_no_hitchhikers(example_env, guide_key)
        self.assert_no_hitchhikers(example_env, encyclopedia_key)
        self.assert_no_hitchhikers(example_env, vogon_poetry_key)


class TestCreateUnknown(TestProtoReferenceBase):
    def test_create_unknown(self, example_env):
        with pytest.raises(NoSuchObjectError):
            self.create_hitchhiker(example_env, 100, {"id": 100, "id2": 100}, None)


class TestCreateDuplicate(TestProtoReferenceBase):
    def test_create_duplicate(self, example_env, guide_key, vogon_poetry_key):
        with pytest.raises(YtResponseError):
            self.create_hitchhiker(
                example_env,
                100,
                self.book_ids(example_env, vogon_poetry_key),
                [
                    self.book_ids(example_env, vogon_poetry_key),
                    self.book_ids(example_env, guide_key),
                ],
            )


class TestUpdateAdd(TestProtoReferenceBase):
    def test_update_add(self, example_env, arthur, encyclopedia_key):
        example_env.client.update_object(
            "hitchhiker",
            str(arthur),
            set_updates=[
                {
                    "path": "/spec/hated_books/end",
                    "value": self.book_ids(example_env, encyclopedia_key),
                },
            ],
        )
        self.assert_hitchhiker_ids(example_env, encyclopedia_key, [arthur])


class TestUpdateRemove(TestProtoReferenceBase):
    def test_update_remove(self, example_env, ford, encyclopedia_key):
        example_env.client.update_object(
            "hitchhiker", str(ford), remove_updates=[{"path": "/spec/hated_books/0"}]
        )
        self.assert_no_hitchhikers(example_env, encyclopedia_key)


class TestUpdateDuplicate(TestProtoReferenceBase):
    def test_update_duplicate(self, example_env, arthur, guide_key):
        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "hitchhiker",
                str(arthur),
                set_updates=[
                    {
                        "path": "/spec/hated_books/end",
                        "value": self.book_ids(example_env, guide_key),
                    },
                ],
            )


class TestUpdatePermutation(TestProtoReferenceBase):
    def test_update_permutation(
        self, example_env, ford, guide_key, encyclopedia_key, vogon_poetry_key
    ):
        example_env.client.update_object(
            "hitchhiker",
            str(ford),
            set_updates=[
                {
                    "path": "/spec/favorite_book",
                    "value": self.book_ids(example_env, encyclopedia_key),
                },
                {
                    "path": "/spec/hated_books",
                    "value": [
                        self.book_ids(example_env, vogon_poetry_key),
                        self.book_ids(example_env, guide_key),
                    ],
                },
            ],
        )
        self.assert_hitchhiker_ids(example_env, guide_key, [ford])
        self.assert_hitchhiker_ids(example_env, encyclopedia_key, [ford])
        self.assert_hitchhiker_ids(example_env, vogon_poetry_key, [ford])


class TestNoAddBackref(TestProtoReferenceBase):
    def test_no_add_backref(self, example_env, zaphod, guide_key):
        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "book",
                guide_key,
                set_updates=[
                    {"path": "/status/hitchhikers/end", "value": zaphod},
                ],
            )


class TestNoRemoveBackref(TestProtoReferenceBase):
    def test_no_remove_backref(self, example_env, arthur, guide_key):
        with pytest.raises(YtResponseError):
            example_env.client.update_object(
                "book",
                guide_key,
                remove_updates=[
                    {"path": "/status/hitchhikers/0"},
                ],
            )
