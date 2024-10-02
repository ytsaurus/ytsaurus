from .conftest import sync_access_control

from yt.orm.library.common import AuthorizationError

import pytest


@pytest.fixture
def example_user_id(example_env):
    user_id = example_env.client.create_object("user")
    sync_access_control()
    return user_id


@pytest.fixture
def example_user_client(example_env, example_user_id):
    with example_env.create_client(
        config=dict(enable_ssl=False, user=example_user_id),
    ) as client:
        yield client


# Object "employer" /meta and /status and "book" /meta contain etc and non-etc attributes for tests completeness.
class TestAttributesUpdatableBySuperuserOnly:
    def _create_employer(self, client, id, transaction_id=None):
        return client.create_object(
            "employer",
            attributes=dict(
                meta=dict(id=id, passport="1234 567890", email="employer@yandex.ru"),
            ),
            transaction_id=transaction_id,
        )

    def _update(
        self, object_type, client, object_key, top_level, transaction_id=None, **kwargs
    ):
        updates = []
        for name in kwargs:
            updates.append(
                dict(
                    path="/{}/{}".format(top_level, name),
                    value=kwargs[name],
                ),
            )
        assert updates
        client.update_object(
            object_type, object_key, set_updates=updates, transaction_id=transaction_id
        )

    def _update_employer(self, *args, **kwargs):
        self._update("employer", *args, **kwargs)

    def _update_book(self, *args, **kwargs):
        self._update("book", *args, **kwargs)

    def _grant_write_permission(self, example_env, object_type, object_key, user_id, attribute):
        example_env.client.update_object(
            object_type,
            object_key,
            set_updates=[
                dict(
                    path="/meta/acl/end",
                    value=dict(
                        subjects=[user_id],
                        permissions=["write"],
                        attributes=[attribute],
                        action="allow",
                    ),
                ),
            ],
        )
        sync_access_control()

    def test_create(self, example_env, example_user_id, example_user_client):
        example_env.client.update_object(
            "schema",
            "employer",
            set_updates=[
                dict(
                    path="/meta/acl/end",
                    value=dict(subjects=[example_user_id], permissions=["create"], action="allow"),
                ),
            ],
        )
        sync_access_control()

        # User is not allowed to update attributes updatable by superuser only even within a transaction creating the object.
        for passport, email in ((None, "b"), ("a", None), ("a", "b")):
            transaction_id = example_user_client.start_transaction()
            employer_id = self._create_employer(example_user_client, "Sergey", transaction_id)
            with pytest.raises(AuthorizationError):
                self._update_employer(
                    example_user_client,
                    employer_id,
                    "meta",
                    transaction_id=transaction_id,
                    passport=passport,
                    email=email,
                )

        # Sanity check.
        transaction_id = example_user_client.start_transaction()
        employer_id = self._create_employer(example_user_client, "Sergey", transaction_id)
        example_user_client.remove_object("employer", employer_id, transaction_id=transaction_id)
        example_user_client.commit_transaction(transaction_id)

        # User is allowed to initialize attributes updatable by superuser only.
        self._create_employer(example_user_client, "Petr")

    # Mechanics implementation differs for /meta and /status.
    def test_update_meta(self, example_env, example_user_id, example_user_client):
        employer_id = self._create_employer(example_env.client, "Ivan")

        # Superuser updates /meta.
        passport = "321 321"
        email = "ivan2@yandex.ru"
        self._update_employer(example_env.client, employer_id, "meta", passport=passport, email=email)

        # User without permission updates /meta.
        with pytest.raises(AuthorizationError):
            self._update_employer(example_user_client, employer_id, "meta", uuid="123")
        with pytest.raises(AuthorizationError):
            self._update_employer(example_user_client, employer_id, "meta", name="xxx")
        with pytest.raises(AuthorizationError):
            self._update_employer(
                example_user_client, employer_id, "meta", passport=passport[:-1], email=email[:-1]
            )
        with pytest.raises(AuthorizationError):
            self._update_employer(example_user_client, employer_id, "meta", email=email[:-1])
        with pytest.raises(AuthorizationError):
            self._update_employer(example_user_client, employer_id, "meta", passport=passport[:-1])

        # User with permission updates /meta.
        self._grant_write_permission(
            example_env, "employer", employer_id, example_user_id, "/meta"
        )
        with pytest.raises(AuthorizationError):
            self._update_employer(example_user_client, employer_id, "meta", uuid="123")
        self._update_employer(example_user_client, employer_id, "meta", name="xxx")
        with pytest.raises(AuthorizationError):
            self._update_employer(
                example_user_client, employer_id, "meta", passport=passport[:-1], email=email[:-1]
            )
        with pytest.raises(AuthorizationError):
            self._update_employer(example_user_client, employer_id, "meta", email=email[:-1])
        with pytest.raises(AuthorizationError):
            self._update_employer(example_user_client, employer_id, "meta", passport=passport[:-1])

        # Superuser and user update /meta of book without attributes_updatable_by_superuser_only option as in previous tests.
        publisher_key = example_env.client.create_object("publisher", request_meta_response=True)
        book_key = example_env.client.create_object(
            "book",
            {"meta": {"isbn": "978-1449355739", "parent_key": publisher_key}},
            request_meta_response=True,
        )

        isbn = "abracadabra"
        custom_meta_etc_test = "some_test"
        self._update_book(
            example_env.client, book_key, "meta", isbn=isbn, custom_meta_etc_test=custom_meta_etc_test
        )

        with pytest.raises(AuthorizationError):
            self._update_book(
                example_user_client,
                book_key,
                "meta",
                isbn=isbn[:-1],
                custom_meta_etc_test=custom_meta_etc_test[:-1],
            )
        with pytest.raises(AuthorizationError):
            self._update_book(example_user_client, book_key, "meta", isbn=isbn[:-1])
        with pytest.raises(AuthorizationError):
            self._update_book(
                example_user_client,
                book_key,
                "meta",
                custom_meta_etc_test=custom_meta_etc_test[:-1],
            )

        self._grant_write_permission(example_env, "book", book_key, example_user_id, "/meta")
        self._update_book(example_user_client, book_key, "meta", isbn=isbn[:-1])
        self._update_book(
            example_user_client, book_key, "meta", custom_meta_etc_test=custom_meta_etc_test[:-1]
        )
        self._update_book(
            example_user_client,
            book_key,
            "meta",
            isbn=isbn[:-2],
            custom_meta_etc_test=custom_meta_etc_test[:-2],
        )

    # Mechanics implementation differs for /meta and /status.
    def test_update_status(self, example_env, example_user_id, example_user_client):
        employer_id = self._create_employer(example_env.client, "Vova")

        # Superuser updates /status.
        job_title = "Developer"
        salary = 1
        self._update_employer(
            example_env.client, employer_id, "status", job_title=job_title, salary=salary
        )

        # User without permission updates /status.
        for i in range(2):
            if i == 1:
                self._grant_write_permission(
                    example_env, "employer", employer_id, example_user_id, "/status"
                )
            with pytest.raises(AuthorizationError):
                self._update_employer(
                    example_user_client,
                    employer_id,
                    "status",
                    job_title=job_title[:-1],
                    salary=salary + 1,
                )
            with pytest.raises(AuthorizationError):
                self._update_employer(
                    example_user_client, employer_id, "status", job_title=job_title[:-1]
                )
            with pytest.raises(AuthorizationError):
                self._update_employer(example_user_client, employer_id, "status", salary=salary + 1)
