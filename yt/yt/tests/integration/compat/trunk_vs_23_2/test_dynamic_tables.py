from yt_dynamic_tables_base import DynamicTablesBase

from yt_commands import (
    authors,
    start_transaction, commit_transaction, insert_rows, lookup_rows,
    sync_create_cells, sync_mount_table, create_dynamic_table
)

from yt.common import YtResponseError

import pytest


class TestDynamicTablesCompatibility(DynamicTablesBase):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy", "job-proxy"],
    }

    @authors("ponasenko-rs")
    def test_shared_write_locks_feature(self):
        sync_create_cells(1)

        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "a", "type": "int64", "lock": "la"}
        ]
        create_dynamic_table("//tmp/t", schema=schema)
        sync_mount_table("//tmp/t")

        tx1 = start_transaction(type="tablet")
        tx2 = start_transaction(type="tablet")

        insert_rows("//tmp/t", [{"key": 1, "a": 1}], update=True, lock_type="shared_write", tx=tx1)
        insert_rows("//tmp/t", [{"key": 1, "a": 2}], update=True, lock_type="shared_write", tx=tx2)

        with pytest.raises(YtResponseError, match="Server does not support the feature requested by client"):
            commit_transaction(tx1)
        with pytest.raises(YtResponseError, match="Server does not support the feature requested by client"):
            commit_transaction(tx2)

        assert lookup_rows("//tmp/t", [{"key": 1}], column_names=["key", "a"]) == []
