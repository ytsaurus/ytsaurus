from .helpers import yatest_common
from .orm_test_environment import TO_INITIAL_DB_VERSION

from yt.orm.admin.db_manager import DbManager
from yt.orm.library.common import ClientError

import pytest


class OrmMigrationError(Exception):
    def __init__(self, error, target_db_version):
        self.error = error
        self.target_db_version = target_db_version


class OrmMigrationTestBase(object):
    AVOID_MIGRATIONS = False
    START = False

    # Note: we can't "see" respective project's orm_env fixture from here. Project's test class
    # must provide its own test() function accepting its orm_env fixture and pass it to impl().
    # For example, for YP we need yp_env.
    # ```
    # def test(self, yp_env):
    #    self.impl(yp_env)
    # ```

    def _get_orm_master_binary_path_override(self, data_model_traits, db_version):
        path = data_model_traits.get_orm_master_binary_path_for_tests(db_version)
        if path:
            return yatest_common.runtime.work_path(path)
        return path

    def _try_migrate(self, orm_env, target_db_version, verify_schemas=False):
        try:
            orm_env.orm_instance.migrate(target_db_version, verify_schemas=verify_schemas)
        except Exception as err:
            raise OrmMigrationError(err, target_db_version)


def format_schema_diff(schema1, schema2):
    errors_string = ""
    keys1 = [c for c in schema1 if "sort_order" in c]
    keys2 = [c for c in schema2 if "sort_order" in c]
    for idx, (key1, key2) in enumerate(zip(keys1, keys2)):
        if key1 != key2:
            errors_string += "Key column {} mismatch:\n- {}\n+ {}\n".format(idx, key1, key2)
    if len(keys1) >= len(keys2):
        for idx, key1 in enumerate(keys1[len(keys2):], start=len(keys2)):
            errors_string += "Missing key column {}:\n- {}\n".format(idx, key1)
    if len(keys2) >= len(keys1):
        for idx, key2 in enumerate(keys2[len(keys1):], start=len(keys1)):
            errors_string += "Extra key column {}:\n+ {}\n".format(idx, key2)

    others1 = dict((c["name"], c) for c in schema1 if "sort_order" not in c)
    others2 = dict((c["name"], c) for c in schema2 if "sort_order" not in c)
    for name in set(list(others1) + list(others2)):
        if name in others1:
            if name in others2:
                if others1[name] != others2[name]:
                    errors_string += "Column attributes mismatch:\n- {}\n+ {}\n".format(others1[name], others2[name])
            else:
                errors_string += "Missing column:\n- {}\n".format(others1[name])
        else:
            errors_string += "Extra column:\n+ {}\n".format(others2[name])
    return errors_string


# Compare init_yt_cluster + migrations and init_yt_cluster to ACTUAL_DB_VERSION.
# This test proves that we can use AVOID_MIGRATIONS = true (the default) in all tests
# except the migration tests (because we would get the same empty database on test entry).
class OrmTestMigrationVsInitToActualVersion(OrmMigrationTestBase):
    def impl(self, orm_env):
        yt_client = orm_env.yt_client
        data_model_traits = orm_env.orm_instance._data_model_traits

        db_manager = DbManager(yt_client, orm_env.get_db_path(), data_model_traits)
        db_version = db_manager.read_version()

        # This emulates AVOID_MIGRATIONS = True
        db_manager_without_migrations = DbManager(
            yt_client,
            orm_env.orm_instance.init_yt_cluster_for_tests("_no_migrations", to_actual_version=True),
            data_model_traits
        )
        assert db_version == db_manager_without_migrations.read_version() == data_model_traits.get_actual_db_version()

        assert set(db_manager.get_tables_list()) == set(db_manager_without_migrations.get_tables_list())

        db_manager.unmount_all_tables()
        db_manager_without_migrations.unmount_all_tables()

        def fix_row(row):
            if isinstance(row, dict):
                for key, value in row.items():
                    # Remove known variations in values.
                    if isinstance(key, str) and (key == "uuid" or "time" in key or key == "meta.finalizers"):
                        row[key] = None
                    else:
                        fix_row(value)
            elif isinstance(row, list) or isinstance(row, tuple):
                for value in row:
                    fix_row(value)

        for table in db_manager.get_tables_list():
            assert format_schema_diff(
                db_manager.get_table_schema(table),
                db_manager_without_migrations.get_table_schema(table)
            ) == "", "Schema mismatch for table {}".format(table)

            # Do not read empty tables to make the test faster.
            if (
                yt_client.get(db_manager.get_table_path(table) + "/@chunk_count") == 0
                and yt_client.get(db_manager_without_migrations.get_table_path(table) + "/@chunk_count") == 0
            ):
                continue

            # Both databases should be nearly empty so we can compare tables via read_table().
            rows_migrated = list(db_manager.read_table(table))
            rows_not_migrated = list(db_manager_without_migrations.read_table(table))

            assert len(rows_migrated) == len(rows_not_migrated), "Row count mismatch for table {}".format(table)

            for row_migrated, row_not_migrated in zip(rows_migrated, rows_not_migrated):
                fix_row(row_migrated)
                fix_row(row_not_migrated)
                assert row_migrated == row_not_migrated, "Row mismatch for table {}".format(table)


class OrmTestDBVersionMismatch(OrmMigrationTestBase):
    DB_VERSION = TO_INITIAL_DB_VERSION

    def impl(self, orm_env):
        data_model_traits = orm_env.orm_instance._data_model_traits
        initial_db_version = data_model_traits.get_initial_db_version()
        actual_db_version = data_model_traits.get_actual_db_version()

        assert (
            initial_db_version < actual_db_version
        ), "Initial DB version and actual must differ in order for this test to work"

        orm_env.start(self._get_orm_master_binary_path_override(data_model_traits, initial_db_version))
        orm_env.stop_master()
        if actual_db_version - 1 > initial_db_version:
            self._try_migrate(orm_env, actual_db_version - 1)

        with pytest.raises(ClientError):
            orm_env.start(stop_yt_on_failure=False)

        orm_env.stop_master()
        self._try_migrate(orm_env, actual_db_version)
        orm_env.start()


# Callback #before is executed over the master of #DB_VERSION version and the corresponding database.
# The master executable file is taken from the ya.make DATA() section.
#
# Callback #after is executed over the master of #ACTUAL_DB_VERSION version and the corresponding database.
# The master executable file is built together with the test from the current source code files.
#
# Feel free to override #DB_VERSION in your test to execute #before with a master of a different version.
# It is recommended to upload the master to the ya.make DATA() section from the corresponding ORM master release tag.
#
# By default, #DB_VERSION is set to #TO_INITIAL_DB_VERSION which instructs to resolve it to actual
# #INITIAL_DB_VERSION from project's data_model_traits when it is available.
class OrmTestMigration(OrmMigrationTestBase):
    DB_VERSION = TO_INITIAL_DB_VERSION

    def impl(self, orm_env):
        self.orm_env = orm_env
        data_model_traits = orm_env.orm_instance._data_model_traits
        actual_db_version = data_model_traits.get_actual_db_version()

        orm_env.start(self._get_orm_master_binary_path_override(data_model_traits, orm_env.get_db_version()))
        self.before(orm_env.orm_client, orm_env.yt_client)
        orm_env.stop_master()
        self._try_migrate(orm_env, actual_db_version, verify_schemas=True)

        orm_env.start(self._get_orm_master_binary_path_override(data_model_traits, actual_db_version))
        self.after(orm_env.orm_client, orm_env.yt_client)

    def before(self, orm_client, yt_client):
        raise NotImplementedError()

    def after(self, orm_client, yt_client):
        raise NotImplementedError()


class OrmTestOnlineMigration(object):
    LOCAL_YT_OPTIONS = {"http_proxy_count": 1}

    def impl(self, orm_env):
        self.before(orm_env.orm_client, orm_env.yt_client)
        orm_env.orm_instance.migrate_without_downtime()
        self.after(orm_env.orm_client, orm_env.yt_client)

    def before(self, orm_client, yt_client):
        raise NotImplementedError()

    def after(self, orm_client, yt_client):
        raise NotImplementedError()
