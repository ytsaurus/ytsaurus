from .conftest import get_tests_checks_path

from yt_odin.test_helpers import FileStorage, run_checks, yt_env, generate_unique_id  # noqa

from yt_odin.odinserver.odin import Odin
import yt_odin.odinserver.odin as odin_module

from yt_odin.logserver import FAILED_STATE, PARTIALLY_AVAILABLE_STATE, FULLY_AVAILABLE_STATE

from yt.common import update
from yt.environment.arcadia_interop import yatest_common

import mock

import json
import logging
import os
import shutil
import sys
import tempfile
import time


def get_checks_config():
    return {
        "checks": {
            "available": {},
            "invalid": {},
            "long": {},
            "partial": {},
            "with_invalid_return_value": {},
            "with_options": {
                "options": {
                    "key": "value"
                }
            },
            "with_secrets": {},
        }
    }


class OdinTestEnv(object):
    def __init__(self, client, cluster_name="test_cluster"):
        self.client = client
        self.cluster_name = cluster_name

        odin_logger = logging.getLogger("Odin")
        odin_logger.handlers.append(logging.StreamHandler(sys.stderr))

        storage_file = tempfile.mkstemp()[1]
        self.storage_factory = lambda: FileStorage(storage_file)
        self.storage = self.storage_factory()

        self.socket_path = os.path.join(tempfile.gettempdir(), "{}.sock"
                                        .format(generate_unique_id("_test_odin")))

        self.tmp_checks_path = tempfile.mkdtemp(prefix="tmp_checks_path")
        self.tmp_config_path = os.path.join(self.tmp_checks_path, "config.json")
        self.write_config(get_checks_config())

    def copy_check_to_tmp(self, check):
        tmp_check_dir = os.path.join(self.tmp_checks_path, check)
        os.mkdir(tmp_check_dir)
        if yatest_common is not None:
            checks_build_path = yatest_common.build_path("yt/odin/tests/data/checks")
            shutil.copy(os.path.join(checks_build_path, check, check), tmp_check_dir)
        else:
            check_destination = os.path.join(tmp_check_dir, check)
            shutil.copy(
                os.path.join(get_tests_checks_path(), check, "__main__.py"),
                check_destination)
            os.chmod(check_destination, 0o755)

    def write_config(self, config):
        with open(self.tmp_config_path, "w") as f:
            json.dump(config, f)

    def read_config(self):
        with open(self.tmp_config_path, "r") as f:
            return json.load(f)

    def set_check_enable(self, check, enable):
        config = self.read_config()
        diff = {"cluster_overrides": {self.cluster_name: {check: {"enable": enable}}}}
        config = update(config, diff)
        self.write_config(config)

    def create_odin(self, **kwargs):
        return Odin(self.storage_factory,
                    self.cluster_name,
                    self.client.config["proxy"]["url"],
                    token=None,  # Use root on local cluster
                    checks_path=self.tmp_checks_path,
                    log_server_socket_path=self.socket_path,
                    log_server_max_write_batch_size=256,
                    **kwargs)


@mock.patch("yt_odin.odinserver.odin.AlertsManager", autospec=True)
def test_odin(alerts_manager_mock, yt_env):  # noqa
    odin_env = OdinTestEnv(yt_env.yt_client)
    for check in ("partial", "available", "invalid"):
        odin_env.copy_check_to_tmp(check)

    disabled_checks = ("available", "long", "with_invalid_return_value",
                       "with_options", "with_secrets")
    for check in disabled_checks:
        odin_env.set_check_enable(check, False)

    odin_module.RELOAD_CHECKS = True
    odin = odin_env.create_odin(
        secrets=dict(the_ultimate_question=42),
    )
    run_checks(odin)

    assert "available" not in [check.name for check in odin.checks]

    for check in disabled_checks:
        odin_env.set_check_enable(check, True)

    for check in ("long", "with_invalid_return_value", "with_options", "with_secrets"):
        odin_env.copy_check_to_tmp(check)

    odin.alerts_manager.store_check_result.reset_mock()
    odin_module.RELOAD_CHECKS = True
    odin.reload_configuration()
    run_checks(odin)
    time.sleep(5.0)
    odin.terminate()

    storage = odin_env.storage

    def assert_state(check_name, expected_state):
        actual_state = storage.get_service_states(check_name)[0]
        assert abs(actual_state - expected_state) <= 0.001, \
               "Check '{}': expected {}, got {}".format(check_name, expected_state, actual_state)
        alerts_manager_call_count = 0
        for args, kwargs in odin.alerts_manager.store_check_result.call_args_list:
            if args[0] == check_name:
                alerts_manager_call_count += 1
                result = args[1]
                assert abs(result["state"] - expected_state) <= 0.001, \
                       "AlertsManager record for check '{}': expected {}, got {}" \
                       .format(check_name, expected_state, result["state"])
        assert alerts_manager_call_count == 1, "Alerts manager records for '{}': expected 1, got {}" \
                                               .format(check_name, alerts_manager_call_count)

    for check_name in ("available", "long", "with_options", "with_secrets"):
        assert_state(check_name, FULLY_AVAILABLE_STATE)

    for check_name in ("partial",):
        assert_state(check_name, PARTIALLY_AVAILABLE_STATE)

    for check_name in ("invalid", "with_invalid_return_value"):
        assert_state(check_name, FAILED_STATE)

    assert 5000 <= storage.get_all_records("long")[0].duration <= 10000
