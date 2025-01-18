from copy import deepcopy
import logging
from typing import Any

from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.model import YtTable

from .base import AwaitingResultAction

LOG = logging.getLogger("yt_sync")


class ReshardTableAction(AwaitingResultAction):
    def __init__(self, desired_table: YtTable, actual_table: YtTable):
        super().__init__()
        self._desired_table: YtTable = desired_table
        self._actual_table: YtTable = actual_table

    def do_modify_request(self, batch_client: YtClientProxy) -> Any | None:
        LOG.debug(
            "Desired tablet info for %s: tablet_count=%s, pivot_keys=%s",
            self._actual_table.rich_path,
            self._desired_table.tablet_info.tablet_count,
            self._desired_table.tablet_info.pivot_keys,
        )
        if not self._desired_table.has_tablets:
            LOG.debug("Skip resharding %s, no effective tablets", self._actual_table.rich_path)
            return None

        resharding_properties = dict()
        use_pivot_keys: bool = False
        if self._desired_table.has_pivot_keys:
            if self._desired_table.schema.is_ordered:
                LOG.debug(
                    "Pivot keys not supported for ordered table %s, tablet_count used instead.",
                    self._actual_table.rich_path,
                )
            else:
                use_pivot_keys = True

        if use_pivot_keys:
            resharding_properties["pivot_keys"] = self._desired_table.pivot_keys
        else:
            desired_tablet_count: int = self._desired_table.effective_tablet_count
            actual_tablet_count: int = self._actual_table.effective_tablet_count
            if self._actual_table.is_ordered:
                assert (
                    desired_tablet_count >= actual_tablet_count
                ), "Can't reduce tablet count for existing ordered table"

            resharding_properties["tablet_count"] = desired_tablet_count
            resharding_properties["uniform"] = self._desired_table.schema.is_uniform_distribution_supported
        LOG.warning(
            "Reshard table %s:%s (params=%s)",
            self._actual_table.cluster_name,
            self._actual_table.path,
            resharding_properties,
        )
        return batch_client.reshard_table(self._actual_table.path, **resharding_properties)

    def do_await_request(self, batch_client: YtClientProxy) -> Any:
        return batch_client.get(f"{self._desired_table.path}/@tablet_state")

    def is_required_state(self, result: Any) -> bool:
        return str(result) != "transient"

    def postprocess(self):
        self._actual_table.tablet_info.tablet_count = self._desired_table.tablet_count
        self._actual_table.tablet_info.pivot_keys = deepcopy(self._desired_table.pivot_keys)
