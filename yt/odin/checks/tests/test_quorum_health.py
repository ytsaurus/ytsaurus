# coding=utf-8
from yt_odin.logserver import FULLY_AVAILABLE_STATE
from yt_odin.test_helpers import make_check_dir, configure_and_run_checks

import mock

from datetime import datetime


def test_quorum_health(yt_env):
    proxy_url = yt_env.yt_client.config["proxy"]["url"]
    master_monitoring_port = yt_env.yt_instance._cluster_configuration["master"]["1"][0]["monitoring_port"]
    checks_path = make_check_dir("quorum_health", {"monitoring_port": master_monitoring_port})
    storage = configure_and_run_checks(proxy_url, checks_path)
    status = storage.get_service_states("quorum_health")[-1]
    assert status == FULLY_AVAILABLE_STATE


def test_quorum_health_check_cell_health():
    from yt_odin_checks.lib.quorum_health import (
        Cell, Hydra, check_cell_health, get_cluster_cells_health, OK, WARN, CRIT)

    logger = mock.MagicMock()
    primary = True
    secondary = False
    working_time = datetime.strptime('2019-01-10 12:00:00', '%Y-%m-%d %H:%M:%S')
    night_time = datetime.strptime('2019-01-10 23:01:00', '%Y-%m-%d %H:%M:%S')
    morning_time = datetime.strptime('2019-01-10 09:59:00', '%Y-%m-%d %H:%M:%S')

    def make_hydra(hostname_port_string, state):
        result = Hydra.from_hostname_port_string(hostname_port_string)
        result._state = state
        return result

    fake_socrates = [
        Cell(primary, '100', [
            make_hydra('m01-man.fake.yt.yandex.net:9010', Hydra.LEADING_STATE),
            make_hydra('m02-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m03-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
        ]),
        Cell(secondary, '200', [
            make_hydra('m11-man.fake.yt.yandex.net:9010', Hydra.LEADING_STATE),
            make_hydra('m12-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m13-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
        ]),
    ]

    fake_hume = [
        Cell(primary, '100', [
            make_hydra('m01-man.fake.yt.yandex.net:9010', Hydra.LEADING_STATE),
            make_hydra('m02-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m03-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m04-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m05-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
        ]),
        Cell(secondary, '200', [
            make_hydra('m11-man.fake.yt.yandex.net:9010', Hydra.LEADING_STATE),
            make_hydra('m12-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m13-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m14-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m15-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
        ]),
        Cell(secondary, '300', [
            make_hydra('m21-man.fake.yt.yandex.net:9010', Hydra.LEADING_STATE),
            make_hydra('m22-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m23-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m24-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
            make_hydra('m25-man.fake.yt.yandex.net:9010', Hydra.FOLLOWING_STATE),
        ]),
    ]

    # Два села по 3 мастеров и все ок
    assert check_cell_health(fake_socrates[0].masters, working_time, logger) == OK
    assert check_cell_health(fake_socrates[1].masters, working_time, logger) == OK
    health, _ = get_cluster_cells_health(fake_socrates, working_time, logger)
    assert CRIT not in health
    assert WARN not in health

    # Один мастер в MAINTENANCE_STATE - должен быть WARN
    fake_socrates[0].masters[-1]._state = Hydra.MAINTENANCE_STATE
    assert check_cell_health(fake_socrates[0].masters, working_time, logger) == WARN
    assert check_cell_health(fake_socrates[1].masters, working_time, logger) == OK
    health, _ = get_cluster_cells_health(fake_socrates, working_time, logger)
    assert CRIT not in health
    assert WARN in health

    # Нет одного FOLLOWING в селе с тремя мастерами
    fake_socrates[0].masters[-1]._state = Hydra.OFFLINE_STATE
    assert check_cell_health(fake_socrates[0].masters, working_time, logger) == CRIT
    assert check_cell_health(fake_socrates[1].masters, working_time, logger) == OK
    health, _ = get_cluster_cells_health(fake_socrates, working_time, logger)
    assert CRIT in health
    assert WARN not in health

    # Три села по 5 мастеров и все ок
    assert check_cell_health(fake_hume[0].masters, working_time, logger) == OK
    assert check_cell_health(fake_hume[1].masters, working_time, logger) == OK
    assert check_cell_health(fake_hume[2].masters, working_time, logger) == OK
    health, _ = get_cluster_cells_health(fake_hume, working_time, logger)
    assert CRIT not in health
    assert WARN not in health

    # Нет LEADING
    fake_hume[0].masters[0]._state = Hydra.OFFLINE_STATE
    assert check_cell_health(fake_hume[0].masters, working_time, logger) == CRIT
    assert check_cell_health(fake_hume[1].masters, working_time, logger) == OK
    assert check_cell_health(fake_hume[2].masters, working_time, logger) == OK
    health, _ = get_cluster_cells_health(fake_hume, working_time, logger)
    assert CRIT in health
    assert WARN not in health

    # Нет одного FOLLOWING в селе с пятью мастерами в working_time
    fake_hume[0].masters[0]._state = Hydra.LEADING_STATE
    fake_hume[0].masters[-1]._state = Hydra.OFFLINE_STATE
    assert check_cell_health(fake_hume[0].masters, working_time, logger) == CRIT
    health, _ = get_cluster_cells_health(fake_hume, working_time, logger)
    assert CRIT in health
    assert WARN not in health

    # Нет одного FOLLOWING в селе с пятью мастерами в night_time или morning_time
    fake_hume[0].masters[0]._state = Hydra.LEADING_STATE
    fake_hume[0].masters[-1]._state = Hydra.OFFLINE_STATE
    assert check_cell_health(fake_hume[0].masters, night_time, logger) == WARN
    assert check_cell_health(fake_hume[0].masters, morning_time, logger) == WARN
    health, _ = get_cluster_cells_health(fake_hume, night_time, logger)
    assert CRIT not in health
    assert WARN in health
    health, _ = get_cluster_cells_health(fake_hume, morning_time, logger)
    assert CRIT not in health
    assert WARN in health

    # Нет двух FOLLOWING в селе с пятью мастерами в night_time или morning_time
    fake_hume[0].masters[-1]._state = Hydra.OFFLINE_STATE
    fake_hume[0].masters[-2]._state = Hydra.OFFLINE_STATE
    assert check_cell_health(fake_hume[0].masters, night_time, logger) == CRIT
    assert check_cell_health(fake_hume[0].masters, morning_time, logger) == CRIT
    health, _ = get_cluster_cells_health(fake_hume, night_time, logger)
    assert CRIT in health
    assert WARN not in health
    health, _ = get_cluster_cells_health(fake_hume, morning_time, logger)
    assert CRIT in health
    assert WARN not in health
