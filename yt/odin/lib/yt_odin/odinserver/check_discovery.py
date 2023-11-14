from .check_task import CheckTask

from yt.wrapper.common import update
import yt.logger as yt_logger

from six import iteritems

import os
import json
import random
import logging
from copy import deepcopy


class CheckDiscovery(object):
    def __init__(self, cluster_name, checks_path):
        self._cluster_name = cluster_name
        self._checks_path = checks_path
        self._config = None

    def _parse_config(self):
        config_path = os.path.join(self._checks_path, "config.json")
        with open(config_path, "r") as config_file:
            odin_config = json.load(config_file)
        odin_checks_config = odin_config.get("checks", {})
        cluster_overrides = odin_config.get("cluster_overrides", {}).get(self._cluster_name, {})
        return update(odin_checks_config, cluster_overrides)

    def _make_check(self, check_name, check_path, check_config, check_timeout):
        return Check(
            check_name,
            check_path,
            check_config["options"],
            check_config.get("secrets"),
            check_timeout,
            check_config["env"]
        )

    def _prepare_check_config(self, check_name):
        result = self._config[check_name]
        if "options" not in result:
            result["options"] = {}
        result["options"]["cluster_name"] = self._cluster_name
        if "env" not in result:
            result["env"] = {}
        return result

    def discover_checks(self):
        self._config = self._parse_config()
        enabled_check_names = [check_name for check_name, check_config in iteritems(self._config)
                               if check_config.get("enable", True)]

        check_executables = {}
        for check_name in enabled_check_names:
            check_path = os.path.join(self._checks_path, check_name, check_name)
            if os.path.exists(check_path):
                check_executables[check_name] = check_path
            else:
                yt_logger.warning(
                    "Check is skipped since path does not exist (name: %s, path: %s)",
                    check_name,
                    check_path)

        checks = []
        for check_name, check_path in iteritems(check_executables):
            check_config = self._prepare_check_config(check_name)
            check_timeout = check_config.get("check_timeout")
            checks.append(self._make_check(check_name, check_path, check_config, check_timeout))

        return checks

    def get_config(self):
        return deepcopy(self._config)


class Check(object):
    def __init__(self, name, path, options,
                 secrets=None, timeout=None, environment_variables=None):
        self.name = name
        self._path = path
        self._options = options
        self._secrets = secrets
        if self._secrets is None:
            self._secrets = {}
        self._timeout = timeout
        self._environment_variables = environment_variables
        if self._environment_variables is None:
            self._environment_variables = {}

    @staticmethod
    def _make_task_id():
        return hex(random.randint(0, 2 ** 32 - 1))[2:].rstrip("L").zfill(8)

    def _make_stdin_arguments(self, task_id, current_timestamp, log_server_socket_path,
                              yt_client_params, secrets):
        text_log_server_port = None
        for handler in yt_logger.LOGGER.handlers:
            if isinstance(handler, logging.handlers.SocketHandler):
                text_log_server_port = handler.port
                break

        return dict(
            service=self.name,
            task_id=task_id,
            timestamp=current_timestamp,
            yt_client_params=yt_client_params,
            options=self._options,
            check_log_server_socket_path=log_server_socket_path,
            text_log_server_port=text_log_server_port,
            secrets=update(self._secrets, secrets))

    def start_task(self, default_timeout, current_timestamp, socket_handler, yt_client_params, secrets):
        task_id = self._make_task_id()
        command = [self._path]
        timeout = self._timeout if self._timeout is not None else default_timeout
        stdin_arguments = self._make_stdin_arguments(
            task_id,
            current_timestamp,
            socket_handler.socket_path,
            yt_client_params,
            secrets)
        return CheckTask.start(task_id, self.name, command, current_timestamp, socket_handler,
                               stdin_arguments, timeout, self._environment_variables)
