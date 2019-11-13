from .default_configs import get_watcher_config

from yt.common import which, touch

from yt.test_helpers import wait
import yt.subprocess_wrapper as subprocess

import yt.wrapper as yt

import os
import logging

logger = logging.getLogger("Yt.local")

class ProcessWatcher(object):
    def __init__(self,
                 process_pids, process_log_paths,
                 lock_path, config_dir, logs_dir, runtime_dir,
                 config=None, process_runner=None):
        if config is None:
            self._config = get_watcher_config()
        else:
            self._config = config

        self._lock_path = lock_path
        self._log_path = os.path.join(logs_dir, "watcher.log")
        self._state_path = os.path.join(runtime_dir, "logs_rotator/state")
        self._binary_path = self._get_binary_path()
        self._config_path = self._build_config(config_dir, process_pids, process_log_paths)

        if process_runner is None:
            self._process_runner = subprocess.Popen
        else:
            self._process_runner = process_runner

        touch(self._lock_path)
        touch(self._state_path)

        self._process = None

    def start(self):
        self._process = self._process_runner([
            self._binary_path,
            "--lock-file-path", self._lock_path,
            "--logrotate-config-path", self._config_path,
            "--logrotate-state-file", self._state_path,
            "--logrotate-interval", str(self._config["logs_rotate_interval"]),
            "--log-path", self._log_path
        ])

        def watcher_lock_created():
            if self._process.poll() is not None:
                raise yt.YtError("Watcher process unexpectedly terminated with error code {0}".format(self._process.return_code))
            return os.path.exists(self._lock_path)

        wait(watcher_lock_created)

    def stop(self):
        if self._process is not None:
            try:
                os.kill(self._process.pid, 9)
            except OSError:
                logger.exception("Failed to kill watcher instance with pid %d", self._process.pid)

    def _build_config(self, config_dir, process_pids, process_log_paths):
        postrotate_commands = []
        for pid in process_pids:
            postrotate_commands.append(
                # send sighup
                "\t/usr/bin/test -d /proc/{0} && kill -HUP {0} >/dev/null 2>&1 || true".format(pid)
            )

        logrotate_options = [
            "rotate {0}".format(self._config["logs_rotate_max_part_count"]),
            "size {0}".format(self._config["logs_rotate_size"]),
            "missingok",
            "copytruncate",
            "nodelaycompress",
            "nomail",
            "noolddir",
            "compress",
            "create",
            "postrotate",
            "\n".join(postrotate_commands),
            "endscript"
        ]

        config_path = os.path.join(config_dir, "logs_rotator")
        with open(config_path, "w") as config_file:
            for path in process_log_paths:
                config_file.write("{0}\n{{\n{1}\n}}\n\n".format(path, "\n".join(logrotate_options)))

        return config_path

    def _get_binary_path(self):
        watcher_path = which("yt_env_watcher")
        if watcher_path:
            return watcher_path[0]

        watcher_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "bin", "yt_env_watcher"))
        if os.path.exists(watcher_path):
            return watcher_path

        raise yt.YtError("Failed to find yt_env_watcher binary")
