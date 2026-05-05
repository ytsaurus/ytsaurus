import yt.wrapper as yt

from . import actions, app as sequoia_app

import logging
logger = logging.getLogger(__name__)


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
