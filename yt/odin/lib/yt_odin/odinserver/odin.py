from .common import round_down_to_minute, BoundProcess
from .juggler_client import JugglerClient
from .alerts import AlertsManager
from .check_discovery import CheckDiscovery

from yt_odin.logserver import run_logserver, OdinSocketHandler

from six import iteritems

import time
import signal
import logging
import logging.handlers

# Logger of Odin runner
odin_logger = logging.getLogger("Odin")

# Logger of Odin check tasks
tasks_logger = logging.getLogger("Odin tasks")

RELOAD_CHECKS = True


def sighup_handler(signum, frame):
    if signum == signal.SIGHUP:
        global RELOAD_CHECKS
        RELOAD_CHECKS = True


class Odin(object):
    def __init__(self, db_client_factory, cluster_name, proxy, token, checks_path,
                 log_server_socket_path, log_server_max_write_batch_size,
                 yt_request_retry_timeout=3000, yt_request_retry_count=4,
                 yt_heavy_request_retry_timeout=15000, default_check_timeout=65,
                 check_log_messages_max_size=16384, juggler_client_host=None, juggler_client_scheme=None,
                 juggler_client_port=None, juggler_host=None, juggler_responsibles=None,
                 secrets=None, yt_enable_proxy_discovery=True, yt_driver_address_resolver_config=None):
        signal.signal(signal.SIGHUP, sighup_handler)

        self.db_client_factory = db_client_factory
        self.cluster_name = cluster_name
        self.default_check_timeout = default_check_timeout

        self._start_log_server(log_server_socket_path, log_server_max_write_batch_size,
                               check_log_messages_max_size, cluster_name)

        self.discovery = CheckDiscovery(cluster_name, checks_path)
        self.checks = None

        juggler_client = None
        if juggler_client_host is not None:
            juggler_client = JugglerClient(juggler_client_host, juggler_client_port, juggler_client_scheme)

        events_host = proxy if juggler_host is None else juggler_host
        self.alerts_manager = AlertsManager(
            self.create_db_client(),
            events_host,
            juggler_client,
            juggler_responsibles)
        self.reload_configuration()

        retries_policy = {
            "count": yt_request_retry_count,
            "backoff": {
                "policy": "exponential",
                "exponential_policy": {
                    "start_timeout": yt_request_retry_timeout / 4,
                    "max_timeout": yt_request_retry_timeout,
                    "base": 2,
                    "decay_factor_bound": 0.3,
                }
            }
        }
        heavy_retries_policy = {
            "count": yt_request_retry_count,
            "backoff": {
                "policy": "exponential",
                "exponential_policy": {
                    "start_timeout": yt_heavy_request_retry_timeout / 4,
                    "max_timeout": yt_heavy_request_retry_timeout,
                    "base": 2,
                    "decay_factor_bound": 0.3,
                }
            }
        }

        self.yt_client_params = dict(
            config={
                "proxy": {
                    "url": proxy,
                    "retries": retries_policy,
                    "heavy_request_timeout": yt_heavy_request_retry_timeout,
                    "request_timeout": yt_request_retry_timeout,
                    "enable_proxy_discovery": yt_enable_proxy_discovery,
                },
                "write_retries": heavy_retries_policy,
                "read_retries": heavy_retries_policy,
                "dynamic_table_retries": heavy_retries_policy,
                "concatenate_retries": retries_policy,
                "start_operation_retries": retries_policy,
                "token": token,
                "driver_address_resolver_config": yt_driver_address_resolver_config,
            })

        self.secrets = secrets
        if self.secrets is None:
            self.secrets = dict()

        self._tasks = {}

        odin_logger.info("Odin for %s started", proxy)

    def _start_log_server(self, log_server_socket_path, max_write_batch_size, messages_max_size,
                          cluster_name):
        self.logserver_process = BoundProcess(
            target=run_logserver,
            args=(log_server_socket_path, self.create_db_client(), max_write_batch_size,
                  messages_max_size),
            name="Log[{}]".format(cluster_name)
        )
        self.logserver_process.daemon = True
        self.logserver_process.start()
        odin_logger.info("Started Odin log server process (pid: %d)", self.logserver_process.pid)

        self.socket_handler = OdinSocketHandler(log_server_socket_path)
        self.socket_handler.setLevel(logging.INFO)
        tasks_logger.addHandler(self.socket_handler)

    def create_db_client(self):
        return self.db_client_factory()

    def reload_configuration(self):
        self.discover_checks()
        self.alerts_manager.reload_alerts_configs(self.discovery.get_config())

    def discover_checks(self):
        global RELOAD_CHECKS
        if RELOAD_CHECKS:
            self.checks = self.discovery.discover_checks()
            odin_logger.info("Discovered %d checks: %s", len(self.checks),
                             ", ".join([check.name for check in self.checks]))
            RELOAD_CHECKS = False

    def run(self):
        rounded_last_timestamp = round_down_to_minute(time.time())
        while True:
            current_timestamp = time.time()
            rounded_current_timestamp = round_down_to_minute(current_timestamp)
            if rounded_current_timestamp > rounded_last_timestamp:
                self.alerts_manager.send_alerts()
                self.reload_configuration()
                self.start_tasks(current_timestamp)
                rounded_last_timestamp = rounded_current_timestamp
            self._finalize_tasks(current_timestamp)
            time.sleep(3)

    def start_tasks(self, current_timestamp):
        odin_logger.info("Starting new checks")
        for check in self.checks:
            task = check.start_task(self.default_check_timeout, current_timestamp,
                                    self.socket_handler, self.yt_client_params, self.secrets)
            self._tasks[task.id] = task
            odin_logger.info("Odin for %s starts check %s with pid %d and id %s",
                             self.cluster_name,
                             task.service,
                             task.process.pid,
                             task.id)

    def _finalize_tasks(self, current_timestamp):
        tasks_to_remove = []

        for task_id, task in iteritems(self._tasks):
            if task.try_process_result():
                tasks_to_remove.append(task_id)
                continue

            if current_timestamp - task.start_timestamp > task.timeout:
                odin_logger.info("Check %s timed out, killing check process", task_id)
                task.terminate()
                tasks_to_remove.append(task_id)

        for task_id in tasks_to_remove:
            odin_logger.info("Finalizing check process (check id: %s)", task_id)
            self._tasks[task_id].finalize()
            odin_logger.info("Successfully finalized check process (check id: %s)", task_id)
            self.alerts_manager.store_check_result(self._tasks[task_id].service,
                                                   self._tasks[task_id].get_result())
            del self._tasks[task_id]

    def wait_for_tasks(self):  # for tests
        while self._tasks:
            self._finalize_tasks(time.time())
            time.sleep(3)

    def terminate(self):  # for tests
        tasks_logger.removeHandler(self.socket_handler)
        self.logserver_process.terminate()
        self.alerts_manager.terminate()
