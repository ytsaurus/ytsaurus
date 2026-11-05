from typing import Any

import yt.wrapper as yt

from . import actions, app as sequoia_app

import logging
logger = logging.getLogger(__name__)


def get_tablet_cell_peer(app: sequoia_app.SequoiaTool, bundle: str) -> dict[str, Any]:
    cell_ids = app.ground_client.get(
        f"//sys/tablet_cell_bundles/{bundle}/@tablet_cell_ids")
    if not cell_ids:
        raise ValueError(f'Bundle "{bundle}" has no tablet cells')
    cell_id = cell_ids[0]

    if app.ground_client.get(f"//sys/tablet_cells/{cell_id}/@health") != "good":
        raise ValueError(f'Cell {cell_id} is not in good state')

    peers = app.ground_client.get(f"//sys/tablet_cells/{cell_id}/@peers")
    if not peers:
        raise ValueError(f'Cell {cell_id} has empty peer set')

    return peers[0]


def list_master_cell_tags(yt_client: yt.YtClient) -> list[str]:
    return ([
        str(
            yt_client.get(
                "//@native_cell_tag",
                suppress_transaction_coordinator_sync=True,
                suppress_upstream_sync=True))
    ] +
        list(
            yt_client.list(
                "//sys/secondary_masters",
                suppress_transaction_coordinator_sync=True,
                suppress_upstream_sync=True)))


def promote_reign(app: sequoia_app.SequoiaTool, reign: int) -> None:
    ground_config = app.config_provider.get_ground_config()
    path = sequoia_app.make_ground_reign_path(ground_config.sequoia_root_cypress_path)
    action = actions.SetAttributeAction(path, reign)
    logger.info(action.describe())
    if app.options.dry_run:
        action.dry_run(app)
    else:
        action.execute(app)
