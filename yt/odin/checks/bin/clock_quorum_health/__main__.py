# coding=utf-8
from datetime import datetime

from yt_odin_checks.lib.check_runner import main
from yt_odin_checks.lib.quorum_health import WARN, CRIT
from yt_odin_checks.lib.quorum_health import configure_timeout_and_retries
from yt_odin_checks.lib.quorum_health import configure_loggers
from yt_odin_checks.lib.quorum_health import discover_clock
from yt_odin_checks.lib.quorum_health import check_cell_health
from yt_odin_checks.lib.quorum_health import build_juggler_message


def run_check(yt_client, logger, options, states):
    configure_timeout_and_retries(yt_client)
    configure_loggers()

    # Если не получилось получить схема мастеров - возвращаем CRIT
    clock = discover_clock(yt_client, options, logger)
    health = check_cell_health(clock, datetime.now(), logger)
    if health == CRIT:
        return states.UNAVAILABLE_STATE, build_juggler_message(clock)

    if health == WARN:
        return states.PARTIALLY_AVAILABLE_STATE, build_juggler_message(clock)

    return states.FULLY_AVAILABLE_STATE, 'OK'


if __name__ == "__main__":
    main(run_check)
