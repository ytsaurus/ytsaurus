"""Sequoia ground cluster state migration logic."""

from . import actions, app as sequoia_app, migrations

import logging
logger = logging.getLogger(__name__)


def migrate_ground(
    app: sequoia_app.SequoiaTool,
    ground_reign: int,
    target_reign: int,
) -> None:
    """Sequentially execute ground cluster state migrations."""
    assert target_reign > ground_reign

    logging.info("Started ground cluster state migration: "
                 f"{ground_reign}->{target_reign}")

    reign_range = range(ground_reign + 1, target_reign + 1)
    plans = [
        migrations.MIGRATION_PLANNERS[r](app)
        for r in reign_range
    ]

    logger.info("Will execute actions plans: "
                f"{', '.join(p.name for p in plans)}")

    for plan in plans:
        actions.run_action_plan(plan, app)
