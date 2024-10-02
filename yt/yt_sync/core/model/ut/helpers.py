from copy import deepcopy
import uuid

import pytest

from yt.yt_sync.core.model.replica import YtReplica
from yt.yt_sync.core.model.types import Types


class YtReplicaTestBase:
    @pytest.fixture
    @staticmethod
    def replica_id() -> str:
        return str(uuid.uuid4())

    @pytest.fixture
    @staticmethod
    def replica_attributes(table_path: str) -> Types.Attributes:
        return {
            "cluster_name": "remote0",
            "replica_path": table_path,
            "state": "enabled",
            "mode": "sync",
            "enable_replicated_table_tracker": True,
        }

    @pytest.fixture
    @staticmethod
    def full_replica_attributes(replica_attributes: Types.Attributes) -> Types.Attributes:
        result = deepcopy(replica_attributes)
        result["content_type"] = "data"
        result["my_attr"] = 1
        return result


def make_replica(cluster_name: str, table_path: str, replica_mode: str, enable_rtt: bool = False) -> YtReplica:
    return YtReplica(
        replica_id=str(uuid.uuid4()),
        cluster_name=cluster_name,
        replica_path=table_path,
        enabled=True,
        mode=replica_mode,
        enable_replicated_table_tracker=enable_rtt,
    )
