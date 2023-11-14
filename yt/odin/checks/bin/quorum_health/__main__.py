#!/usr/bin/env python3
# coding=utf-8
from datetime import datetime

from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.quorum_health import WARN, CRIT
from yt_odin_checks.lib.quorum_health import configure_timeout_and_retries
from yt_odin_checks.lib.quorum_health import configure_loggers
from yt_odin_checks.lib.quorum_health import discover_masters
from yt_odin_checks.lib.quorum_health import get_cluster_cells_health


def run_check(yt_client, logger, options, states):
    configure_timeout_and_retries(yt_client)
    configure_loggers()

    # Если не получилось получить схема мастеров - возвращаем CRIT
    masters_cells = discover_masters(yt_client, options, logger)
    if masters_cells is None:
        juggler_message = 'Can\'t discover masters'
        return states.UNAVAILABLE_STATE, juggler_message

    health, juggler_message = get_cluster_cells_health(masters_cells, datetime.now(), logger)

    if CRIT in health:
        logger.info(juggler_message)
        return states.UNAVAILABLE_STATE, juggler_message

    if WARN in health:
        logger.info(juggler_message)
        return states.PARTIALLY_AVAILABLE_STATE, juggler_message

    return states.FULLY_AVAILABLE_STATE, 'OK'


if __name__ == "__main__":
    main(run_check)
