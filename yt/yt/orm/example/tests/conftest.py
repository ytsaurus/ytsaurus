from yt.yt.orm.example.python.admin.data_model_traits import (
    EXAMPLE_DATA_MODEL_TRAITS,
)
from yt.yt.orm.example.python.admin.db_operations import reset_example
from yt.yt.orm.example.python.local.local import ExampleInstance

from yt.yt.orm.example.python.common.logger import logger

from yt.orm.admin.db_manager import DbManager

from yt.orm.tests.helpers import (  # noqa
    Cli,
    create_user as create_user_impl,
    create_user_client as create_user_client_impl,
)

from yt.orm.tests.orm_test_environment import (
    HistoryMigrationState,
    OrmTestEnvironment,
    DummyTestClass,
    create_single_test_environment,
    create_test_suite_environment,
)

import yt.wrapper as yt

import logging
import os
import pytest
import time


logger.setLevel(logging.DEBUG)


def format_object_key(object_id):
    parts = str(object_id).split(";")
    if len(parts) == 2:
        return {"id": int(parts[0]), "id2": int(parts[1])}
    return {"id": object_id}


# Keep in sync with `database_options.proto` configuration.
OLD_HISTORY_TABLE = "history_events"
OLD_HISTORY_INDEX = "history_index"
OLD_HISTORY = (OLD_HISTORY_TABLE, OLD_HISTORY_INDEX)
NEW_HISTORY_TABLE = "history_events_v2"
NEW_HISTORY_INDEX = "history_index_v2"
NEW_HISTORY = (NEW_HISTORY_TABLE, NEW_HISTORY_INDEX)


EXAMPLE_MASTER_CONFIG_TEMPLATE = {
    "yt_connector": {"user": "example"},
    "object_manager": {
        "index_mode_per_name": {
            "books_by_year": "enabled",
            "editors_by_achievements": "building",
        },
        "removed_objects_sweep_period": 2000,
        "removed_objects_grace_timeout": 4000,
        "removed_object_table_reader": {
            "read_by_tablets": True,
            "batch_size": 7,
        },
        "default_parents_table_mode": "dont_write_to_common_table",
    },
    "watch_manager": {
        "distribution_policy": {
            "per_type": {
                "executor": {
                    "type": "hash",
                },
            },
        },
    },
    "access_control_manager": {
        "cluster_state_allowed_object_types": ["schema"],
    },
    "transaction_manager": {
        "read_phase_hard_limit": 15,
        "build_key_expression": True,
    },
    "object_service": {
        "enforce_read_permissions": True,
        "tracing_mode": "force",
    },
}

EXAMPLE_CLIENT_CONFIG_TEMPLATE = dict(enable_ssl=False)


class ExampleTestEnvironment(OrmTestEnvironment):
    TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
    SANDBOX_PATH_FROM_OS_ENVIRONMENT = True

    DATA_MODEL_TRAITS = EXAMPLE_DATA_MODEL_TRAITS

    BINARY_RELATIVE_PATHS = [
        "yt/yt/orm/example/server/bin/ytserver-orm-example",
    ]

    logger = logger

    def get_orm_master_config_template(self):
        return EXAMPLE_MASTER_CONFIG_TEMPLATE

    def get_orm_client_config_template(self):
        return EXAMPLE_CLIENT_CONFIG_TEMPLATE

    def get_orm_master_instance_cls(self):
        return ExampleInstance

    def reset_orm(self):
        reset_example(self.example_client)

    # Utility functions for creating various objects.

    def clear_dummy_content_table(self, table_name):
        return self.clear_table(table_name, lambda key: key not in ["dummy", "hash"])

    def create_publisher(
        self,
        illustrator_in_chief=None,
        publisher_group=None,
        publisher_id=None,
        transaction_id=None,
        editor_id=None,
        name="O'Relly",
        enable_structured_response=False,
    ):
        attributes = {"spec": {"name": name}, "meta": {}}
        if illustrator_in_chief is not None:
            attributes["spec"]["illustrator_in_chief"] = illustrator_in_chief
        if publisher_group is not None:
            attributes["spec"]["publisher_group"] = publisher_group
        if publisher_id is not None:
            attributes["meta"]["id"] = publisher_id
        if editor_id is not None:
            attributes["spec"]["editor_in_chief"] = editor_id
        result = self.example_client.create_object(
            "publisher",
            attributes,
            enable_structured_response=True,
            request_meta_response=True,
            transaction_id=transaction_id,
        )

        if enable_structured_response:
            return result
        else:
            return result["meta"]["id"]

    def update_publisher_name(self, publisher_id, name, client=None, transaction_id=None):
        if client is None:
            client = self.client

        return client.update_object(
            "publisher",
            str(publisher_id),
            set_updates=[
                {
                    "path": "/spec/name",
                    "value": name,
                },
            ],
            transaction_id=transaction_id,
        )

    def create_editor(self, name="Greg", labels=None):
        if labels is None:
            labels = {}

        return self.example_client.create_object(
            "editor",
            attributes={
                "spec": {
                    "name": name,
                },
                "labels": labels,
            },
        )

    def create_illustrator(
        self, publisher_id=None, name="Greg", part_time_job=0, transaction_id=None
    ):
        if publisher_id is None:
            publisher_id = self.create_publisher()
        return self.example_client.create_object(
            "illustrator",
            attributes={
                "meta": {"parent_key": str(publisher_id), "part_time_job": part_time_job},
                "spec": {
                    "name": name,
                },
            },
            transaction_id=transaction_id,
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["uid"]

    def create_book(
        self,
        publisher_id=None,
        book_id=None,
        transaction_id=None,
        spec=None,
        enable_structured_response=False,
    ):
        if publisher_id is None:
            publisher_id = self.create_publisher(transaction_id=transaction_id)
        if spec is None:
            spec = {}

        attributes = {
            "meta": {
                "isbn": "978-1449355739",
                "parent_key": str(publisher_id),
            },
            "spec": spec,
        }
        if book_id is not None:
            attributes["meta"]["id"] = int(book_id.split(";")[0])
            attributes["meta"]["id2"] = int(book_id.split(";")[1])

        return self.example_client.create_object(
            "book",
            attributes,
            enable_structured_response=enable_structured_response,
            request_meta_response=True,
            transaction_id=transaction_id,
        )

    def append_book_available_format(
        self, book_id, format="pdf", file_size=0, client=None, transaction_id=None
    ):
        if client is None:
            client = self.client

        return client.update_object(
            "book",
            str(book_id),
            set_updates=[
                {
                    "path": "/spec/digital_data/available_formats/end",
                    "value": {
                        "format": format,
                        "size": file_size,
                    },
                },
            ],
            transaction_id=transaction_id,
        )

    def create_books_with_publishers(self, count):
        publishers = self.example_client.create_objects(
            (("publisher", dict()) for i in range(count))
        )
        return self.example_client.create_objects(
            (("book", dict(meta=dict(parent_key=str(publisher)))) for publisher in publishers)
        )

    def create_mother_ship(self, nexus_id=None, spec=None, transaction_id=None):
        if nexus_id is None:
            nexus_id = self.example_client.create_object(
                "nexus",
                request_meta_response=True,
                transaction_id=transaction_id,
            )

        if spec is None:
            spec = {}

        return self.example_client.create_object(
            "mother_ship",
            {"meta": {"nexus_id": int(nexus_id)}, "spec": spec},
            request_meta_response=True,
            transaction_id=transaction_id,
        )

    def create_author(self, name="Greg", labels=None, age=18):
        attributes = {"spec": {"name": name, "age": age}}
        if labels is not None:
            attributes["labels"] = labels
        return self.example_client.create_object(
            "author",
            attributes=attributes,
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["id"]

    def create_cat(self, spec=None, transaction_id=None):
        return self.example_client.create_object(
            "cat",
            {"spec": spec if spec is not None else {}},
            request_meta_response=True,
            transaction_id=transaction_id,
        )

    def create_genre(self, name, transaction_id=None):
        attributes = {
            "spec": {
                "name": name,
            },
        }

        return self.example_client.create_object(
            "genre",
            attributes,
            enable_structured_response=True,
            request_meta_response=True,
            transaction_id=transaction_id,
        )["meta"]["key"]

    def grant_permission(
        self,
        object_type,
        object_key,
        permission,
        user_id,
        attribute,
        update_root=False,
        transaction_id=None,
    ):
        new_ace = dict(
            subjects=[user_id],
            permissions=[permission],
            attributes=[attribute],
            action="allow",
        )
        set_updates = [
            dict(
                path="/meta/acl/end",
                value=new_ace,
            )
        ]
        if update_root:
            set_updates = [
                dict(
                    path="/meta/acl",
                    value=[new_ace],
                )
            ]
        self.client.update_object(
            object_type,
            object_key,
            set_updates=set_updates,
            transaction_id=transaction_id,
        )
        sync_access_control()

    def create_object(self, object_type, spec=None, meta=None, transaction_id=None):
        return self.example_client.create_object(
            object_type,
            attributes={
                "meta": meta if meta is not None else {},
                "spec": spec if spec is not None else {},
            },
            transaction_id=transaction_id,
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["key"]

    def clean_history(self):
        db_path = self.db_manager.get_db_path()
        for history in (OLD_HISTORY, NEW_HISTORY):
            for table in history:
                self.clear_table(yt.ypath_join(db_path, table))


class ExampleTestEnvironmentWithReplicas(ExampleTestEnvironment):
    def __init__(self, test_cls=DummyTestClass, replica_instance_count=1):
        super(ExampleTestEnvironmentWithReplicas, self).__init__(
            test_cls,
            replica_instance_count=replica_instance_count,
        )

        self.yt_replica_clients = self.example_instance.create_replica_clients()
        self.replicas_db_managers = [
            DbManager(
                client,
                EXAMPLE_DATA_MODEL_TRAITS.get_default_path(),
                EXAMPLE_DATA_MODEL_TRAITS,
                self._db_version,
            )
            for client in self.yt_replica_clients
        ]


@pytest.fixture(scope="class")
def test_environment(request):
    return create_test_suite_environment(ExampleTestEnvironment, request)


@pytest.fixture(scope="class")
def test_environment_with_replicas(request):
    return create_test_suite_environment(ExampleTestEnvironmentWithReplicas, request)


@pytest.fixture
def example_env(request, test_environment):
    return create_single_test_environment(request, test_environment)


@pytest.fixture
def example_env_with_replicas(request, test_environment_with_replicas):
    return create_single_test_environment(request, test_environment_with_replicas)


@pytest.fixture
def orm_env(example_env):
    return example_env


def sync_access_control():
    time.sleep(10)


class User:
    def __init__(self, user_id, client):
        self.id = user_id
        self.client = client


@pytest.fixture
def regular_user1(example_env):
    user_id = example_env.client.create_object("user")
    with example_env.create_client(
        config=dict(enable_ssl=False, user=user_id),
    ) as client:
        yield User(user_id, client)


@pytest.fixture
def regular_user2(example_env):
    user_id = example_env.client.create_object("user")
    with example_env.create_client(
        config=dict(enable_ssl=False, user=user_id),
    ) as client:
        yield User(user_id, client)


def create_user(test_environment, *args, **kwargs):
    return create_user_impl(
        test_environment, test_environment.example_client, sync_access_control, *args, **kwargs
    )


def create_user_client(test_environment, superuser=False, grant_permissions=None):
    return create_user_client_impl(
        test_environment,
        test_environment.example_client,
        test_environment.example_instance,
        sync_access_control,
        superuser=superuser,
        grant_permissions=grant_permissions,
    )


@pytest.fixture(
    params=[
        (HistoryMigrationState.INITIAL, OLD_HISTORY),
        (HistoryMigrationState.TARGET, NEW_HISTORY),
    ],
    ids=[OLD_HISTORY_TABLE, NEW_HISTORY_TABLE],
)
def for_both_history_tables(request, example_env: ExampleTestEnvironment):
    example_env.set_history_migration_state(request.param[0])
    return request.param[1]
