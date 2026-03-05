from typing import Dict

from yt.admin.ytsaurus_ci import models


class CheckRegistry:
    def __init__(self, checks: Dict[str, models.Check]):
        self._check_registry = {}
        for name, descriptor in checks.items():
            self._check_registry[name] = descriptor

    def get_check(self, name) -> models.Check:
        return self._check_registry[name]
