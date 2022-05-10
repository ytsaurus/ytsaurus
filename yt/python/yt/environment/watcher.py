from .default_config import get_watcher_config
from .helpers import is_file_locked

from yt.common import which, touch, update

from yt.test_helpers import wait
import yt.subprocess_wrapper as subprocess

import yt.wrapper as yt

import os
import sys
import logging

try:
    import yt_env_watcher
except ImportError:
    yt_env_watcher = None

logger = logging.getLogger("YtLocal")


class ProcessWatcher(object):
    def __init__(self,
                 watcher_binary,
                 process_pids, process_log_paths,
                 lock_path, config_dir, logs_dir, runtime_dir,
                 config=None, process_runner=None):
        self._config = get_watcher_config()
        if config is not None:
            for k, v in config.items():
                self._config[k] = v

        self._lock_path = lock_path
        self._log_path = os.path.join(logs_dir, "watcher.log")
        self._state_path = os.path.join(runtime_dir, "logs_rotator/state")
        if watcher_binary:
            self._binary_path = watcher_binary
        elif yt_env_watcher is None:
            self._binary_path = self._get_binary_path()
        else:
            self._binary_path = None

        self._config_path = self._build_config(config_dir, process_pids, process_log_paths)

        if process_runner is None:
            self._process_runner = subprocess.Popen
        else:
            self._process_runner = process_runner

        touch(self._lock_path)
        touch(self._state_path)

        self._process = None

    def start(self):
        env = None
        if self._binary_path is None:
            watcher_cmd = [sys.executable]
            # If self._process_runner receives env which is not None, then the existing environment
            # variables are not inherited. That's not what we expect. Watcher needs to inherit PATH
            # at least in order to locate logrotate binary. So, we add the existing environment.
            env = update(os.environ, {"Y_PYTHON_ENTRY_POINT": "__yt_env_watcher_entry_point__"})
        elif getattr(sys, "is_standalone_binary", False):
            watcher_cmd = [self._binary_path]
        else:  # Use current binary python for yt_env_watcher execution.
            watcher_cmd = [sys.executable, self._binary_path]

        watcher_cmd += [
            "--lock-file-path", self._lock_path,
            "--logrotate-config-path", self._config_path,
            "--logrotate-state-file", self._state_path,
            "--logrotate-interval", str(self._config["logs_rotate_interval"]),
            "--log-path", self._log_path,
        ]
        if self._config.get("disable_logrotate"):
            watcher_cmd += ["--disable-logrotate"]

        self._process = self._process_runner(watcher_cmd, env=env)

        def watcher_lock_created():
            if self._process.poll() is not None:
                raise yt.YtError("Watcher process unexpectedly terminated with error code {0}".format(self._process.returncode))
            return is_file_locked(self._lock_path)

        wait(watcher_lock_created)

    def stop(self):
        if self._process is not None:
            try:
                os.kill(self._process.pid, 9)
            except OSError:
                logger.exception("Failed to kill watcher instance with pid %d", self._process.pid)

    def get_pid(self):
        return self._process.pid

    def _build_config(self, config_dir, process_pids, process_log_paths):
        postrotate_commands = []
        for pid in process_pids:
            postrotate_commands.append(
                # send sighup
                "\t/usr/bin/test -d /proc/{0} && kill -HUP {0} >/dev/null 2>&1 || true".format(pid)
            )

        compress_options = ["nodelaycompress", "compress"] if self._config.get("logs_rotate_compress") else []
        logrotate_options = [
            "rotate {0}".format(self._config["logs_rotate_max_part_count"]),
            "size {0}".format(self._config["logs_rotate_size"]),
            "missingok",
            "copytruncate",
            "nomail",
            "noolddir",
            "create",
        ] + compress_options + [
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
