from .conftest import ExampleTestEnvironment

from yt.orm.tests.helpers import MigrateAttributesCli as OrmMigrateAttributesCli

import yatest.common


class ExampleMigrateAttributesCli(OrmMigrateAttributesCli):
    def __init__(self):
        super().__init__(yatest.common.binary_path("yt/yt/orm/example/tools/migrate_attributes/migrate_attributes"))


class TestAttributeMigrationOperation:
    def test_publisher(self, example_env: ExampleTestEnvironment):
        PUBLISHERS_TABLE_PATH = "//home/example/db/publishers"

        client = example_env.client
        yt_client = example_env.yt_client

        publisher = client.create_object(
            "publisher",
            {"spec": {"address": "Yekaterinburg", "non_column_field": 18}},
            enable_structured_response=True,
            request_meta_response=True,
        )["meta"]["id"]

        # Manual row update, that simulates old master.
        rows = list(yt_client.select_rows(f"* from [{PUBLISHERS_TABLE_PATH}] WHERE [meta.id] = {publisher} LIMIT 1"))
        assert len(rows) == 1, "Object row not found"
        del rows[0]["hash"]
        rows[0]["spec.etc"] = {"address": "Moscow", "non_column_field": 23}
        yt_client.insert_rows(PUBLISHERS_TABLE_PATH, rows)

        result = client.get_object("publisher", str(publisher), ["/status/address", "/spec/column_field"])
        assert ["Yekaterinburg", 18] == result

        ExampleMigrateAttributesCli.run(
            example_env,
            "publisher",
            object_key_columns=["meta.id"],
            target_attributes=["/status/address", "/spec/column_field"],
        )

        result = client.get_object("publisher", str(publisher), ["/status/address", "/spec/column_field"])
        assert ["Moscow", 23] == result
