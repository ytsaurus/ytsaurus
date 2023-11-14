from yt_odin.test_helpers import FileStorage

from yt_odin.odinserver.alerts import AlertsManager, PeriodStatus

from yt_odin.logserver import (
    FULLY_AVAILABLE_STATE,
    PARTIALLY_AVAILABLE_STATE,
    UNAVAILABLE_STATE
)

import datetime
import tempfile


def get_checks_config():
    return {
        "checks": {
            "with_partially_available_strategy": {
                "alerts": {
                    "skip": {
                        "partially_available_strategy": "skip",
                        "period": 5,
                        "threshold": 3
                    },
                    "force_ok": {
                        "partially_available_strategy": "force_ok",
                        "period": 5,
                        "threshold": 3
                    },
                    "warn": {
                        "partially_available_strategy": "warn",
                        "period": 5,
                        "threshold": 3
                    }
                }
            },
            "with_overrides": {
                "alerts": {
                    "simple": {
                        "juggler_service_name": "test_check",
                        "period": 2,
                        "threshold": 24,
                        "time_period_overrides": [
                            {
                                "start_time": "{:02}:00:00".format(i),
                                "end_time": "{:02}:00:00".format(i + 1),
                                "threshold": i + 1
                            }
                            for i in range(23)
                        ]
                    }
                }
            },
            "without_alerts_config": {
                "enable": True,
            },
            "with_long_period": {
                "alerts": {
                    "simple": {
                        "period": 200,
                        "threshold": 1,
                    }
                },
            },
            "ok_check": {
                "alerts": {
                    "simple": {
                        "period": 5,
                        "threshold": 4,
                    }
                },
            },
            "failed_check": {
                "alerts": {
                    "simple": {
                        "period": 5,
                        "threshold": 2,
                    }
                },
            },
            "warn_check": {
                "alerts": {
                    "simple": {
                        "partially_available_strategy": "warn",
                        "period": 5,
                        "threshold": 2,
                    }
                },
            },
        }
    }


class FakeJugglerClient(object):
    def __init__(self):
        self.events = []

    def push_events(self, events):
        for ev in events:
            self.events.append(dict(status=ev["status"], service=ev["service"]))


def test_alerts_manager():
    storage_file = tempfile.mkstemp()[1]
    storage = FileStorage(storage_file)
    host = "localhost"
    juggler_client = FakeJugglerClient()
    juggler_responsibles = ["you@host.domain"]

    alerts_manager = AlertsManager(storage, host, juggler_client, juggler_responsibles)
    alerts_manager.reload_alerts_configs(get_checks_config()["checks"])

    assert "without_alerts_config" not in alerts_manager._alerts_configs
    assert alerts_manager._alerts_configs["with_overrides"]["simple"].get_threshold() \
           == datetime.datetime.now().hour + 1

    alerts_config = alerts_manager._alerts_configs["with_partially_available_strategy"]
    assert alerts_manager._get_period_status([1, 0.5, 0.5, 1, 0.5], alerts_config["skip"]) == PeriodStatus.OK
    assert alerts_manager._get_period_status([1, 0.5, 0.5, 1, 2], alerts_config["skip"]) == PeriodStatus.CRIT
    assert alerts_manager._get_period_status([1, 0.5, 0.5, 1, 2], alerts_config["force_ok"]) == PeriodStatus.OK
    assert alerts_manager._get_period_status([1, 0.5, 0.5, 1, 2], alerts_config["warn"]) == PeriodStatus.WARN

    def store_check_states(check, states):
        for state in states:
            alerts_manager.store_check_result(check, dict(state=state, description=None))

    store_check_states("ok_check", [FULLY_AVAILABLE_STATE] * 5)
    store_check_states("failed_check", [FULLY_AVAILABLE_STATE] + [UNAVAILABLE_STATE] * 4)
    store_check_states("warn_check", [FULLY_AVAILABLE_STATE] + [PARTIALLY_AVAILABLE_STATE] * 4)
    store_check_states("with_long_period", [UNAVAILABLE_STATE] * 100)

    alerts_manager.send_alerts()
    alerts_manager.terminate()

    assert len(juggler_client.events) == 3

    def assert_alert(check, status):
        event = dict(service=check, status=status)
        assert event in juggler_client.events, "Event not found: {}".format(event)

    assert_alert("ok_check", "OK")
    assert_alert("failed_check", "CRIT")
    assert_alert("warn_check", "WARN")
