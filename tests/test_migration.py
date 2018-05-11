import pytest
import time
from yp.client import to_proto_enum

import proto.yp.client.api.proto.data_model_pb2 as data_model_pb2

from yt.wrapper import YtResponseError
from yt.wrapper.dynamic_table_commands import _waiting_for_tablets
from yt.environment.helpers import wait

def create_row_mapping(rows, key):
    result = {}
    for row in rows:
        result[row[key]] = row
    return result

def wait_result(callback, iter=100, sleep_backoff=0.3):
    for _ in xrange(iter - 1):
        try:
            return callback()
        except:
            time.sleep(sleep_backoff)
    return callback()

@pytest.mark.usefixtures("yp_env_migration")
class TestMigration(object):
    def test_1to2(self, yp_env_migration):
        yt_client = yp_env_migration.yt_client

        resource_rows = [
            {"meta.node_id": "NODE1", "labels": {}, "meta.id": "R1", "spec": {"kind": "disk", "disk": {}, "total_capacity": 1000000000L}},
            {"meta.node_id": "NODE2", "labels": {}, "meta.id": "R2", "spec": {"kind": "cpu", "cpu": {}, "total_capacity": 1000L}},
            {"meta.node_id": "NODE3", "labels": {}, "meta.id": "R3", "spec": {"kind": "memory", "memory": {}, "total_capacity": 1000000L}},
            {"meta.node_id": "NODE4", "labels": {}, "meta.id": "R4", "spec": {"kind": "disk", "disk": {}, "total_capacity": 1000000000L}},
        ]
        yt_client.insert_rows("//yp/db/resources", resource_rows)

        assert not yt_client.exists("//yp/db/virtual_services")

        yp_env_migration.yp_instance.migrate_database(2, backup_path="//yp.backup")

        assert yt_client.exists("//yp/db/virtual_services")

        new_resource_rows = wait_result(lambda: yt_client.select_rows("* from [//yp/db/resources]"))

        pre_resources = create_row_mapping(resource_rows, key="meta.id")
        new_resources = create_row_mapping(new_resource_rows, key="meta.id")

        for meta_id, row in new_resources.items():
            pre_row = pre_resources[meta_id]
            assert row["meta.kind"] == to_proto_enum(data_model_pb2.EResourceKind, pre_row["spec"]["kind"]).number
            assert len(row["spec"]) == 1
            assert "cpu" in row["spec"] or "memory" in row["spec"]
            assert row["spec"][pre_row["spec"]["kind"]]["total_capacity"] == pre_row["spec"]["total_capacity"]

        for meta_id, row in pre_resources.items():
            if row["spec"]["kind"] == "disk":
                assert meta_id not in new_resources

        yt_client.mount_table("//yp.backup/db/resources", sync=True)

        backup_resource_rows = wait_result(lambda: yt_client.select_rows("* from [//yp.backup/db/resources]"))
        backup_resources = create_row_mapping(backup_resource_rows, key="meta.id")
        assert len(backup_resources) == len(pre_resources)

        for meta_id, row in pre_resources.items():
            for key in ["meta.node_id", "labels", "meta.id", "spec"]:
                assert row[key] == backup_resources[meta_id][key]
