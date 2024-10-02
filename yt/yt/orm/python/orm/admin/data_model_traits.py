from .configure_db_bundle import create_tablet_balancer_schedule

from abc import ABCMeta, abstractmethod
import importlib


# TODO: Turn into @dataclass when py2 is gone.
class MigrationWithoutDowntime(object):
    def __init__(
        self,
        dynconfig_field,
        # Possible values for dynconfig_field.
        before_migration,
        writing_new_way,
        reading_new_way,
        after_migration,
        # Called after setting dynconfig_field into writing_new_way and before reading_new_way.
        migration_function,
        verification_function=None,
        tables_to_flush=[],
    ):
        self.dynconfig_field = dynconfig_field
        self.before_migration = before_migration
        self.writing_new_way = writing_new_way
        self.reading_new_way = reading_new_way
        self.after_migration = after_migration
        self.migration_function = migration_function
        self.verification_function = verification_function
        self.tables_to_flush = tables_to_flush


class DataModelTraits(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._migration_methods = self.collect_migration_methods()
        self._yt_schemas = {}
        self._migrations_without_downtime = self.collect_migrations_without_downtime()

    @abstractmethod
    def get_human_readable_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_snake_case_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_initial_db_version(self):
        raise NotImplementedError

    @abstractmethod
    def get_actual_db_version(self):
        raise NotImplementedError

    def supports_replication(self):
        return False

    def get_kebab_case_name(self):
        return self.get_snake_case_name().replace("_", "-")

    def get_account(self):
        return self.get_kebab_case_name()

    def get_user_name(self):
        return self.get_account()

    def get_default_path(self):
        return "//home/{}".format(self.get_snake_case_name())

    def get_used_media(self):
        return ("default",)

    def patch_table_attributes(self, table, attributes, version):
        return attributes

    @abstractmethod
    def get_yt_schema(self, version):
        raise NotImplementedError

    def _get_yt_schema(self, version, schemas_path):
        assert version >= self.get_initial_db_version()
        assert version <= self.get_actual_db_version()
        if version not in self._yt_schemas:
            self._yt_schemas[version] = importlib.import_module(
                "{}{}".format(schemas_path, version)
            )
        return self._yt_schemas[version].YT_SCHEMA

    def get_table_schemas(self, version):
        return self.get_yt_schema(version)["all_tables"]

    def get_table_schema(self, version, table):
        schemas = self.get_table_schemas(version)
        assert table in schemas, "Generated schema for {} version {} not found".format(
            table, version
        )
        assert table not in self.get_table_schema_verification_exclude_list(version)
        return schemas[table]["schema"]

    def get_table_attributes(self, version, table):
        schemas = self.get_table_schemas(version)
        assert table in schemas, "Generated schema for {} version {} not found".format(
            table, version
        )
        return schemas[table].get("attributes", {})

    def get_table_schema_verification_exclude_list(self, version):
        return []

    def get_orm_master_binary_path_for_tests(self, version):
        return None

    def get_per_cluster_tablet_balancer_schedule(self):
        return {
            "markov": create_tablet_balancer_schedule(8),
        }

    def collect_migration_methods(self):
        migration_methods = {}
        for i in range(self.get_initial_db_version() + 1, self.get_actual_db_version() + 1):
            name = "migrate_to_v_" + str(i)
            method = getattr(self, name)
            migration_methods[i] = method
        return migration_methods

    def get_migration_methods(self):
        return self._migration_methods

    def collect_migrations_without_downtime(self):
        return {}

    def get_migrations_without_downtime(self):
        return self._migrations_without_downtime

    def backup_expiration_timeout(self):
        return 0  # Unlimited TTL.

    def backup_checkpoint_timestamp_delay(self):
        return None

    def backup_checkpoint_check_timeout(self):
        return None

    # If returns None, yt list + skip_watch_log=True + exclude_tables/include_tables will be used
    def get_tables_for_backup(self):
        return None
