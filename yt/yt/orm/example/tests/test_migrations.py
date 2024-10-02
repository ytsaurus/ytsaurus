# Tests for downtime-style migrations.
# https://yt.yandex-team.ru/docs/orm/description/migration#migration-with-downtime

from yt.yt.orm.example.python.admin.data_model_traits import (
    ACTUAL_DB_VERSION,
    INITIAL_DB_VERSION,
)

import pytest

# This a special python magic that makes pytest's verbose assert functionality
# available inside yt.orm.tests.migrations.
pytest.register_assert_rewrite("yt.orm.tests.migrations")

from yt.orm.tests.migrations import (  # noqa
    OrmTestMigrationVsInitToActualVersion,
    OrmTestDBVersionMismatch,
    OrmTestMigration,
)


class TestMigrationVsInitToActualVersion(OrmTestMigrationVsInitToActualVersion):
    def test(self, example_env):
        self.impl(example_env)


def test_migration_test_completeness():
    for version in range(INITIAL_DB_VERSION + 1, ACTUAL_DB_VERSION + 1):
        test_name = "TestMigrationV{}".format(version)
        assert test_name in globals(), "Missing test {} for migration {}".format(
            test_name,
            version,
        )


# Note: This test can not be used and must be commented out in these cases:
# 1. There are no migrations, i.e. INITIAL_DB_VERSION == ACTUAL_DB_VERSION.
# 2. Project flag version_compatibility is set to VC_DO_NOT_VALIDATE.
# class TestDBVersionMismatch(OrmTestDBVersionMismatch):
#     def test(self, example_env):
#         self.impl(example_env)


class ExampleTestMigration(OrmTestMigration):
    def test(self, example_env):
        self.impl(example_env)


class TestMigrationV2(ExampleTestMigration):
    def before(self, client, yt_client):
        self._old_book = self.orm_env.create_book(spec={"name": "Old Book", "year": 1820, "in_stock": 53})
        self._new_book = self.orm_env.create_book(spec={"name": "New Book", "year": 1920, "in_stock": 11})

    def after(self, client, yt_client):
        assert client.get_object("book", self._old_book, selectors=["/spec/in_stock"])[0] == 0
        assert client.get_object("book", self._new_book, selectors=["/spec/in_stock"])[0] == 11
