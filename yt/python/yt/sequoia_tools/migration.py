"""Sequoia ground cluster state migration logic."""

from . import actions, app as sequoia_app, helpers, migrations

import logging
logger = logging.getLogger(__name__)


def migrate_ground(
    app: sequoia_app.SequoiaTool,
    ground_reign: int,
    target_reign: int,
) -> None:
    """Sequentially execute ground cluster state migrations."""
    if target_reign == ground_reign:
        logger.info("Ground reign is already equal to target: %d", ground_reign)
        return

    assert target_reign > ground_reign

    logger.info(
        "Started ground cluster state migration: %d -> %d",
        ground_reign,
        target_reign)

    reign_range = range(ground_reign + 1, target_reign + 1)
    plans = {
        r: migrations.MIGRATION_PLANNERS[r](app)
        for r in reign_range
    }

    logger.info("Will execute actions plans: %s",
                ", ".join(p.name for p in plans.values()))

    for reign, plan in plans.items():
        actions.run_action_plan(plan, app)
        helpers.promote_reign(app, reign)
