from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from .table_attributes import YtTableAttributes


@dataclass
class YtTabletInfo:
    tablet_count: int | None
    pivot_keys: list[Any] | None = None
    tablet_balancer_config: YtTableAttributes | None = None

    @property
    def has_pivot_keys(self) -> bool:
        if not self.pivot_keys:
            return False
        if len(self.pivot_keys) == 1 and not self.pivot_keys[0]:
            return False
        return True

    @property
    def has_tablets(self) -> bool:
        return self.has_pivot_keys or bool(self.tablet_count)

    def is_resharding_required(self, desired: YtTabletInfo) -> bool:
        desired_auto_reshard_enabled = desired.tablet_balancer_config.get("enable_auto_reshard", False)
        if desired_auto_reshard_enabled:
            return False
        if desired.pivot_keys and self.pivot_keys != desired.pivot_keys:
            return True
        if desired.tablet_count and self.tablet_count != desired.tablet_count:
            return True
        return False

    @staticmethod
    def make(
        tablet_count: int | None, pivot_keys: list[Any] | None, tablet_balancer_config: dict[str, Any]
    ) -> YtTabletInfo:
        effective_config = deepcopy(tablet_balancer_config)
        if tablet_balancer_config:
            effective_config = {"enable_auto_reshard": True, "enable_auto_tablet_move": True}
            effective_config.update(tablet_balancer_config)
        return YtTabletInfo(
            tablet_count=tablet_count,
            pivot_keys=pivot_keys,
            tablet_balancer_config=YtTableAttributes.make(effective_config),
        )
