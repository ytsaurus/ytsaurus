from dataclasses import dataclass
from typing import Optional, Protocol, cast

import yt.wrapper as yt

from .config import ConfigProvider
from .helpers import make_ground_reign_path

import logging
logger = logging.getLogger(__name__)


class UserInteraction(Protocol):
    def confirm(self, message: str, default: bool = False) -> bool:
        """Ask user for confirmation."""
        pass


@dataclass(frozen=True)
class SequoiaToolOptions:
    dry_run: bool


class SequoiaTool():
    """Root object for the application."""

    def __init__(
        self,
        remote_client: yt.YtClient,
        ground_client: yt.YtClient,
        config_provider: ConfigProvider,
        interaction: UserInteraction,
        **kwargs,
    ) -> None:
        self.remote_client = remote_client
        self.ground_client = ground_client
        self.config_provider = config_provider
        self.interaction = interaction
        self.options = SequoiaToolOptions(**kwargs)

    def try_get_current_reign(self) -> Optional[int]:
        config = self.config_provider.get_ground_config()
        path = make_ground_reign_path(config.sequoia_root_cypress_path)
        try:
            value = self.ground_client.get(path)
            return cast(int, value)
        except yt.errors.YtResolveError:
            logger.warning(f"Ground reign is not set at {path}")
            return None

    def try_get_target_reign(self) -> Optional[int]:
        address = self.remote_client.list("//sys/primary_masters")[0]
        path = f"//sys/primary_masters/{address}/orchid/ground_reign"
        try:
            value = self.remote_client.get(path)
            return cast(int, value)
        except yt.errors.YtResolveError:
            # COMPAT(danilalexeev)
            logger.warning(f"Ground reign is missing at {path}")
            return None
