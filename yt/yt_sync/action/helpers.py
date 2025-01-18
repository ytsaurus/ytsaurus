from copy import deepcopy
from typing import Any

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes

from .base import ActionBase


def reset_actual_table_after_create(
    actual_table: YtTable, desired_table: YtTable, external_attributes: YtTableAttributes | None = None
):
    actual_table.exists = True
    actual_table.table_type = desired_table.table_type
    actual_table.schema = deepcopy(desired_table.schema)
    if external_attributes:
        actual_table.attributes = deepcopy(external_attributes)
    else:
        actual_table.attributes = deepcopy(desired_table.attributes)
    actual_table.tablet_state.set(None)
    actual_table.replicas.clear()
    # collocation_id can be cached in desired table in case when table is recreated
    actual_table.replication_collocation_id = desired_table.replication_collocation_id
    actual_table.tablet_info.tablet_count = 1
    actual_table.tablet_info.pivot_keys = None


def replica_attributes_for_create(replicated_table: YtTable, replica: YtReplica) -> Types.Attributes:
    attributes = replica.yt_attributes
    attributes["table_path"] = replicated_table.path
    return attributes


def create_replica_from(table: YtTable, desired_replica: YtReplica, replica_attrs: Types.Attributes) -> YtReplica:
    replica = YtReplica(
        replica_id=None,
        cluster_name=table.cluster_name,
        replica_path=table.path,
        enabled=desired_replica.enabled,
        mode=YtReplica.Mode.ASYNC,
        enable_replicated_table_tracker=desired_replica.enable_replicated_table_tracker,
        content_type=desired_replica.content_type,
        attributes=deepcopy(desired_replica.attributes),
        exists=False,
        is_temporary=table.is_temporary,
    )
    replica.update_from_attributes(replica_attrs)
    return replica


def parse_tablet_infos(response: Any | None) -> list[dict]:
    ActionBase.assert_response(response)
    assert response
    tablets = dict(response.get_result()).get("tablets")
    assert tablets
    return list(tablets)
