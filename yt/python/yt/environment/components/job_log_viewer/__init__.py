import logging
import os

from yt.common import YtError, wait
from yt.environment import YTComponent

import yt.packages.requests as requests

logger = logging.getLogger("YtLocal")

JOB_LOGS_PATH = "//home/yt-job-logs"
READY_TIMEOUT = 60
READY_PROBE_TIMEOUT = 1


class JobLogViewer(YTComponent):
    """Run a local job-log-viewer with a supplied binary path; it reads the environment through RPC proxies."""

    LOWERCASE_NAME = "job_log_viewer"

    def __init__(self):
        self.env = None
        self.address = None
        self.cluster = None
        self.pid = None

    def prepare(self, env, config):
        if config.get("count", 1) != 1:
            raise YtError("Only one job-log-viewer per environment is supported")
        if env.yt_config.rpc_proxy_count == 0:
            raise YtError("job-log-viewer reads YT through RPC proxies, start the environment with rpc_proxy_count >= 1")

        logger.info("Preparing job-log-viewer")
        self.env = env
        self.binary = os.path.abspath(config["path"])
        endpoint = "127.0.0.1:{}".format(next(env._open_port_iterator))
        self.address = "http://" + endpoint
        # Requests must name the cluster by this exact string, and the YT recipe exports the proxy as localhost:<port>.
        self.cluster = "localhost:" + env.get_http_proxy_address().rsplit(":", 1)[1]

        viewer_config = {
            "http_addr": endpoint,
            "debug_http_addr": "127.0.0.1:{}".format(next(env._open_port_iterator)),
            "metrics_http_addr": "127.0.0.1:{}".format(next(env._open_port_iterator)),
            "auth": {
                "debug_login": "root",
                "disable_tvm": True,
            },
            "logviewer": {
                "cluster": self.cluster,
                "logs_table": JOB_LOGS_PATH + "/logs",
                "logs_meta_table": JOB_LOGS_PATH + "/logs_meta",
                "jobs_meta_table": JOB_LOGS_PATH + "/jobs_meta",
                "disable_tvm": True,
            },
        }
        (self.config_path,) = env.prepare_external_component(
            self.binary, self.LOWERCASE_NAME, "job log viewers", [viewer_config], force_overwrite=True, config_format="json"
        )

    def run(self):
        self.pid = self.env.run_external_component(
            self.LOWERCASE_NAME,
            [self.binary, "--config", self.config_path, "--log-to-stderr"],
        )

    def wait(self):
        def ready():
            self.env._validate_processes_are_running(self.LOWERCASE_NAME)
            try:
                return requests.get(self.address + "/ready", timeout=READY_PROBE_TIMEOUT).ok
            except (requests.ConnectionError, requests.Timeout):
                return False

        wait(ready, timeout=READY_TIMEOUT)
        logger.info("job-log-viewer ready at %s for cluster %s", self.address, self.cluster)

    def init(self):
        pass

    def stop(self):
        if self.pid is not None:
            self.env.kill_service(self.LOWERCASE_NAME)
            self.pid = None
