import logging
import os

from yt.common import YtError, wait
from yt.environment import YTComponent
from yt.environment.helpers import write_config

import yt.packages.requests as requests
import yt.subprocess_wrapper as subprocess
import yt.yson as yson

logger = logging.getLogger("YtLocal")


class StrawberryController(YTComponent):
    """Run a local controller with a supplied binary path and controller-family configs."""

    LOWERCASE_NAME = "strawberry_controller"

    def __init__(self):
        self.env = None
        self.address = None
        self.pid = None

    def prepare(self, env, config):
        if config.get("count", 1) != 1:
            raise YtError("Only one Strawberry controller per environment is supported")

        logger.info("Preparing Strawberry controller")
        self.env = env
        self.binary = os.path.abspath(config["path"])
        proxy = env.get_http_proxy_address()
        root = config.get("strawberry_root", "//sys/strawberry")
        user = config.get("robot_username", "root")
        token = config.get("token", "")
        controllers = config["controllers"]
        endpoint = "127.0.0.1:{}".format(next(env._open_port_iterator))
        self.address = "http://" + endpoint

        controller_config = {
            "token": token,
            "location_proxies": [proxy],
            "http_api_endpoint": endpoint,
            "http_monitoring_endpoint": "127.0.0.1:{}".format(next(env._open_port_iterator)),
            "http_metrics_endpoint": "127.0.0.1:{}".format(next(env._open_port_iterator)),
            "disable_api_auth": config.get("disable_api_auth", True),
            "strawberry": {
                "root": root,
                "stage": config.get("stage", "production"),
                "robot_username": user,
            },
            "controllers": controllers,
            "http_controller_mappings": config["http_controller_mappings"],
        }
        # Reuse the freshly allocated endpoints when restarting a preserved sandbox.
        (self.config_path,) = env.prepare_external_component(
            self.binary, self.LOWERCASE_NAME, "Strawberry controllers", [controller_config], force_overwrite=True
        )

        # run_components() calls init() after run()/wait(), but Cypress must be
        # initialized before the controller starts reconciling operations.
        init_config_path = os.path.join(env.configs_path, "strawberry_controller_init.yson")
        write_config(
            {
                "token": token,
                "proxy": proxy,
                "strawberry_root": root,
                "robot_username": user,
                "families": list(controllers),
            },
            init_config_path,
        )
        init_log_path = os.path.join(env.logs_path, "strawberry_controller_init.log")
        with open(init_log_path, "wb") as log:
            subprocess.check_call(
                [self.binary, "init-cluster", "--config-path", init_config_path, "--log-to-stderr"],
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=60,
            )

    def run(self):
        self.pid = self.env.run_external_component(
            self.LOWERCASE_NAME,
            [self.binary, "run", "--config-path", self.config_path, "--log-to-stderr"],
        )

    def check_liveness(self):
        self.env._validate_processes_are_running(self.LOWERCASE_NAME)

    def wait(self):
        def ready():
            self.check_liveness()
            try:
                return requests.get(self.address + "/ping", timeout=1).ok
            except (requests.ConnectionError, requests.Timeout):
                return False

        wait(ready, timeout=60)
        logger.info("Strawberry controller ready at %s", self.address)

    def init(self):
        pass

    def request(self, command, **params):
        response = requests.post(
            "{}/{}/{}".format(self.address, self.env.get_http_proxy_address(), command),
            data=yson.dumps({"params": params, "unparsed": False}),
            headers={"Content-Type": "application/yson", "X-YT-TestUser": "root"},
            timeout=10,
        )
        result = yson.loads(response.content, encoding="utf-8")
        if not response.ok:
            raise RuntimeError("Strawberry {} failed: {}".format(command, result))
        return result.get("result")

    def stop(self):
        if self.pid is not None:
            self.env.kill_service(self.LOWERCASE_NAME)
            self.pid = None
