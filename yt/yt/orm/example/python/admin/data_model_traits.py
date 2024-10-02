from yt.yt.orm.example.python.admin.autogen import schema as db_schema

from yt.orm.admin.configure_db_bundle import create_tablet_balancer_schedule
from yt.orm.admin.data_model_traits import DataModelTraits


INITIAL_DB_VERSION = 1
ACTUAL_DB_VERSION = db_schema.DB_VERSION


class ExampleDataModelTraits(DataModelTraits):
    def get_human_readable_name(self):
        return "Example"

    def get_snake_case_name(self):
        return "example"

    def get_account(self):
        return "default"

    def get_user_name(self):
        return self.get_snake_case_name()

    def supports_replication(self):
        return True

    def get_initial_db_version(self):
        return INITIAL_DB_VERSION

    def get_actual_db_version(self):
        return ACTUAL_DB_VERSION

    def get_yt_schema(self, version=ACTUAL_DB_VERSION):
        return db_schema.YT_SCHEMA

    def get_used_media(self):
        return ("ssd_blobs", "default")

    def get_per_cluster_tablet_balancer_schedule(self):
        return {
            "markov": create_tablet_balancer_schedule(8),
        }

    def patch_table_attributes(self, table, attributes, version=ACTUAL_DB_VERSION):
        attributes["periodic_compaction_mode"] = "partition"
        attributes["auto_compaction_period"] = 1000 * 60 * 30
        attributes["primary_medium"] = "ssd_blobs"

    def get_tables_for_backup(self):
        return list(db_schema.YT_SCHEMA["tables"].keys()) + list(
            db_schema.YT_SCHEMA["indexes"].keys()
        )

    def migrate_to_v_2(self, db_manager):
        def mapper(row):
            if row.get("spec.year", -1) <= 1900:
                row["spec.etc$warehouse"]["in_stock"] = 0
            yield row

        db_manager.run_map("books", mapper)


EXAMPLE_DATA_MODEL_TRAITS = ExampleDataModelTraits()
