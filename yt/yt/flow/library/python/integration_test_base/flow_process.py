import os
import json
import random
import requests
import logging
import sys

from enum import Enum
from pathlib import Path
from typing import Optional

import yatest.common

from yt.wrapper import yson
from yt.yt.flow.library.python.bullied_process import BulliedProcess, ProblemsConfig

from .helpers import get_yson_config, dump_yson_config, dump_operation_stderr

log = logging.getLogger("flow_process")

# Can be enabled with --test-param RUNNER_LOG_LEVEL=Debug.
# It is useful for debugging pipeline runner until it use custom program and config.
RUNNER_LOG_LEVEL = yatest.common.get_param("RUNNER_LOG_LEVEL", None)

# Can be enabled with --test-param DUMP_BACKTRACES_IF_HANGS=1.
# Dumps thread and fiber backtraces if flow processes are finishing with active exception from `wait` function.
# Can not be enabled by default because of https://nda.ya.ru/t/C-RYA0Ch7a3i5M.
DUMP_BACKTRACES_IF_HANGS = yatest.common.get_param("DUMP_BACKTRACES_IF_HANGS", "0") == "1"


class FlowProcessMode(str, Enum):
    WORKER = "Worker"
    CONTROLLER = "Controller"


def _dump_sensors_json(url: str, headers: dict[str, str], out_path: str, timeout: float = 30) -> None:
    """Fetch sensors JSON from ``url`` and write the canonical {"sensors": [...]} form to ``out_path``.

    Sensors are sorted and written one-per-line for stable diffs. A timeout guards against the
    monitoring endpoint hanging during teardown.
    """
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    sensors = json.loads(r.text)["sensors"]
    with open(out_path, "w") as f:
        f.write('{"sensors": [\n')
        # One sensor per line.
        f.write(",\n".join(sorted(json.dumps(s, sort_keys=True) for s in sensors)))
        f.write('\n]}\n')


class FlowSimpleProcess(BulliedProcess):
    def __init__(
        self,
        config_path: str | os.PathLike,
        binary_path: str | os.PathLike,
        mode: FlowProcessMode,
        process_index: int,
        logs_dir: str,
        rpc_port: int,
        monitoring_port: int,
        companion_port: int,
        companion_monitoring_port: int,
        env: dict[str, str],
        **kwargs,
    ):
        self.rpc_port = rpc_port
        self.monitoring_port = monitoring_port
        self.companion_port = companion_port
        self.companion_monitoring_port = companion_monitoring_port
        self._logs_dir = logs_dir
        self._ports_dir = os.path.join(logs_dir, "ports")
        self.mode = mode
        self.process_index = process_index

        config_patch = {
            "rpc_port": self.rpc_port,
            "monitoring_port": self.monitoring_port,
            "companion": {
                "port": self.companion_port,
                "monitoring_port": self.companion_monitoring_port,
            },
            "logging": {
                "writers": {
                    name: {
                        "type": "file",
                        "file_name": os.path.join(logs_dir, f"{mode.value}_{process_index}_{name}.log"),
                    }
                    for name, conf in get_yson_config(config_path).get("logging", {}).get("writers", {}).items()
                    if logs_dir not in conf.get("file_name", "")  # TODO: remove after making normal work checking.
                },
            },
        }
        dump_yson_config(config_patch, os.path.join(logs_dir, f"{mode.value}_{process_index}_config_patch.yson"))

        Path(self._ports_dir).mkdir(parents=True, exist_ok=True)
        with open(os.path.join(self._ports_dir, f"{mode.value}_{process_index}_rpc.txt"), "w") as f:
            f.write(str(self.rpc_port))
        with open(os.path.join(self._ports_dir, f"{mode.value}_{process_index}_monitoring.txt"), "w") as f:
            f.write(str(self.monitoring_port))
        with open(os.path.join(self._ports_dir, f"{mode.value}_{process_index}_companion.txt"), "w") as f:
            f.write(str(self.companion_port))
        with open(os.path.join(self._ports_dir, f"{mode.value}_{process_index}_companion_monitoring.txt"), "w") as f:
            f.write(str(self.companion_monitoring_port))

        env = dict(env)
        env["YT_FLOW_MODE"] = mode.value
        env["YT_FLOW_CONFIG"] = yson.dumps(config_patch)
        env["YT_FLOW_COMPANION_LOGGER_TYPE"] = "File"
        env["YT_FLOW_COMPANION_LOG_DIR"] = logs_dir
        env["YT_FLOW_COMPANION_LOG_FILE"] = os.path.join(logs_dir, f"Companion_{process_index}_ALL.log")

        super(FlowSimpleProcess, self).__init__(
            launch_cmd=[binary_path, "--config", config_path],
            env=env,
            check_exit_code=False,
            stdout=os.path.join(self._logs_dir, f"{mode.value}_{process_index}.out"),
            stderr=os.path.join(self._logs_dir, f"{mode.value}_{process_index}.err"),
            name=f"{mode.value}_{process_index}",
            **kwargs,
        )

    def try_dump_process_state(self, debug_hang):
        file_prefix = os.path.join(self._logs_dir, f"{self.mode.value}_{self.process_index}_")

        try:
            # The combined endpoint already includes the embedded companion's sensors.
            _dump_sensors_json(
                f"http://localhost:{self.monitoring_port}/solomon_proxy/sensors",
                headers={},
                out_path=file_prefix + "sensors.json",
            )
        except Exception as e:
            log.error(f"Can't dump sensors of {self.mode.value}_{self.process_index}", exc_info=e)

        if DUMP_BACKTRACES_IF_HANGS and debug_hang:
            for backtrace_type in ("threads", "fibers"):
                try:
                    r = requests.get(f"http://localhost:{self.monitoring_port}/backtrace/{backtrace_type}")
                    with open(file_prefix + f"backtrace_{backtrace_type}.txt", "w") as f:
                        f.write(r.text)
                except Exception as e:
                    log.error(
                        f"Can't dump backtrace/{backtrace_type} of {self.mode.value}_{self.process_index}", exc_info=e
                    )


class FlowCompanionProcess(BulliedProcess):
    """
    FlowCompanionProcess is a companion process that runs alongside a worker process.
    Each companion shares the same companion_port with its paired worker process.
    """

    def __init__(
        self,
        binary_path: str | os.PathLike,
        binary_args: list[str] | None,
        mode: FlowProcessMode,
        process_index: int,
        logs_dir: str,
        companion_port: int,
        companion_monitoring_port: int,
        cluster_url: str,
        pipeline_path: str,
        env: dict[str, str],
        **kwargs,
    ):
        self.companion_port = companion_port
        self.companion_monitoring_port = companion_monitoring_port
        self._logs_dir = logs_dir
        self._ports_dir = os.path.join(logs_dir, "ports")
        self.process_index = process_index

        Path(self._ports_dir).mkdir(parents=True, exist_ok=True)
        with open(os.path.join(self._ports_dir, f"Companion_{process_index}_port.txt"), "w") as f:
            f.write(str(self.companion_port))
        with open(os.path.join(self._ports_dir, f"Companion_{process_index}_monitoring_port.txt"), "w") as f:
            f.write(str(self.companion_monitoring_port))

        companion_config = {
            "port": companion_port,
            "monitoring_port": companion_monitoring_port,
            "cluster_url": cluster_url,
            "pipeline_path": pipeline_path,
        }

        env = dict(env)
        env["YT_FLOW_MODE"] = mode.value
        env["YT_FLOW_COMPANION_CONFIG"] = yson.dumps(companion_config, yson_format="text").decode("utf-8")
        env["YT_FLOW_COMPANION_LOGGER_TYPE"] = "File"
        env["YT_FLOW_COMPANION_LOG_DIR"] = logs_dir
        env["YT_FLOW_COMPANION_LOG_FILE"] = os.path.join(logs_dir, f"Companion_{process_index}_ALL.log")
        env["COMPANION_PROCESS_INDEX"] = str(process_index)

        launch_cmd = [str(binary_path)]
        if binary_args:
            launch_cmd.extend(binary_args)

        super(FlowCompanionProcess, self).__init__(
            launch_cmd=launch_cmd,
            cwd=os.path.dirname(binary_path),
            env=env,
            check_exit_code=False,
            stdout=os.path.join(self._logs_dir, f"Companion_{process_index}.out"),
            stderr=os.path.join(self._logs_dir, f"Companion_{process_index}.err"),
            name=f"Companion_{process_index}",
            **kwargs,
        )

    def try_dump_process_state(self, debug_hang):
        try:
            # The companion's /metrics endpoint relies on Accept-based content negotiation;
            # query parameters are not parsed.
            _dump_sensors_json(
                f"http://localhost:{self.companion_monitoring_port}/metrics",
                headers={"Accept": "application/json"},
                out_path=os.path.join(self._logs_dir, f"Companion_{self.process_index}_sensors.json"),
            )
        except Exception as e:
            log.error(f"Can't dump sensors of Companion_{self.process_index}", exc_info=e)


class FlowSimpleProcessFederation:
    def __init__(
        self,
        binary_path: str | os.PathLike,
        runner_binary_path: str | os.PathLike,
        node_config: dict,
        pipeline_binary_args: dict[str, str],
        logs_dir: str | os.PathLike,
        port_manager,
        env: dict[str, str] | None = None,
        controllers_count: int = 1,
        workers_count: int = 1,
        start_watcher_thread: bool = True,
        controller_problems_config: ProblemsConfig | None = None,
        worker_problems_config: ProblemsConfig | None = None,
        companion_problems_config: ProblemsConfig | None = None,
        run_pipeline: bool = True,
        use_vanilla_jobs: bool = False,
        run_companion_externally: bool = False,
        companion_binary_path: Optional[str | os.PathLike] = None,
        companion_binary_args: Optional[list[str]] = None,
        companion_cluster_url: Optional[str] = None,
        companion_pipeline_path: Optional[str] = None,
        client=None,
    ):
        self._binary_path = binary_path
        self._runner_binary_path = runner_binary_path
        self._pipeline_binary_args = pipeline_binary_args
        self._logs_dir = logs_dir
        self._port_manager = port_manager
        self._run_pipeline = run_pipeline
        self._use_vanilla_jobs = use_vanilla_jobs
        self._run_companion_externally = run_companion_externally
        self._companion_binary_path = companion_binary_path
        self._companion_binary_args = companion_binary_args
        self._companion_cluster_url = companion_cluster_url
        self._companion_pipeline_path = companion_pipeline_path
        self._client = client

        # Validation: companion_binary_path is required when run_companion_externally is True.
        assert (
            not self._run_companion_externally or self._companion_binary_path is not None
        ), "companion_binary_path must be provided when run_companion_externally is True"

        # Validation: companion_cluster_url and companion_pipeline_path are required when
        # run_companion_externally is True
        assert not self._run_companion_externally or (
            self._companion_cluster_url is not None and self._companion_pipeline_path is not None
        ), "companion_cluster_url and companion_pipeline_path must be provided when run_companion_externally is True"

        self._node_config_path = os.path.join(self._logs_dir, "config.yson")
        dump_yson_config(node_config, self._node_config_path)

        self._env = dict(env if env is not None else os.environ)

        seed = os.getenv("RANDOM_SEED")
        if seed is None:
            seed = random.randrange(sys.maxsize)
        else:
            seed = int(seed)

        self._random_generator = random.Random(seed)
        log.info("Initialized random generator with seed=%d", seed)

        self.controllers = []
        self.workers = []
        self.companions = []
        if not self._use_vanilla_jobs:
            self.controllers = self._start_processes(
                FlowProcessMode.CONTROLLER, controllers_count, start_watcher_thread, controller_problems_config
            )
            workers, companions = self._start_worker_processes(
                workers_count, start_watcher_thread, worker_problems_config, companion_problems_config
            )
            self.workers = workers
            self.companions = companions
        self._prepare_start()

    def _start_processes(
        self, mode: FlowProcessMode, count: int, start_watcher_thread: bool, problems_config: ProblemsConfig | None
    ):
        return [
            FlowSimpleProcess(
                binary_path=self._binary_path,
                config_path=self._node_config_path,
                mode=mode,
                process_index=i,
                logs_dir=self._logs_dir,
                rpc_port=self._port_manager.get_port(),
                monitoring_port=self._port_manager.get_port(),
                companion_port=self._port_manager.get_port(),
                companion_monitoring_port=self._port_manager.get_port(),
                env=self._env,
                start_watcher_thread=start_watcher_thread,
                problems_config=problems_config,
                random_generator=self._random_generator,
            )
            for i in range(count)
        ]

    def _start_worker_processes(
        self,
        count: int,
        start_watcher_thread: bool,
        worker_problems_config: ProblemsConfig | None,
        companion_problems_config: ProblemsConfig | None,
    ) -> tuple[list[FlowSimpleProcess], list[FlowCompanionProcess]]:
        """
        Create worker processes and optionally companion processes.
        Each worker-companion pair shares the same companion_port.

        Returns:
            Tuple of (workers list, companions list)
        """
        workers = []
        companions = []

        for i in range(count):
            # Allocate shared companion ports for worker-companion pair.
            companion_port = self._port_manager.get_port()
            companion_monitoring_port = self._port_manager.get_port()

            worker = FlowSimpleProcess(
                binary_path=self._binary_path,
                config_path=self._node_config_path,
                mode=FlowProcessMode.WORKER,
                process_index=i,
                logs_dir=self._logs_dir,
                rpc_port=self._port_manager.get_port(),
                monitoring_port=self._port_manager.get_port(),
                companion_port=companion_port,
                companion_monitoring_port=companion_monitoring_port,
                env=self._env,
                start_watcher_thread=start_watcher_thread,
                problems_config=worker_problems_config,
                random_generator=self._random_generator,
            )
            workers.append(worker)

            if self._run_companion_externally:
                companion = FlowCompanionProcess(
                    binary_path=self._companion_binary_path,
                    binary_args=self._companion_binary_args,
                    mode=FlowProcessMode.WORKER,
                    process_index=i,
                    logs_dir=self._logs_dir,
                    companion_port=companion_port,
                    companion_monitoring_port=companion_monitoring_port,
                    cluster_url=self._companion_cluster_url,
                    pipeline_path=self._companion_pipeline_path,
                    env=self._env,
                    start_watcher_thread=start_watcher_thread,
                    problems_config=companion_problems_config,
                    random_generator=self._random_generator,
                )
                companions.append(companion)

        return workers, companions

    def _prepare_start(self):
        launch_cmd = [self._runner_binary_path]

        for key, value in self._pipeline_binary_args.items():
            launch_cmd.append(key)
            # For non key-value arguments.
            if value is not None:
                launch_cmd.append(value)

        env = dict(self._env)
        if RUNNER_LOG_LEVEL:
            env["YT_LOG_LEVEL"] = RUNNER_LOG_LEVEL

        self._pipeline = BulliedProcess(
            launch_cmd=launch_cmd,
            cwd=os.path.dirname(self._runner_binary_path),
            env=env,
            check_exit_code=True,
            stdout=os.path.join(self._logs_dir, "Runner.out"),
            stderr=os.path.join(self._logs_dir, "Runner.err"),
            start_watcher_thread=True,
            normal_exit_is_ok=True,
        )

    def __enter__(self):
        for process in self.controllers + self.workers + self.companions:
            process.start()

        if self._run_pipeline:
            self.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        self._pipeline.start()

    def wait(self, **kwargs):
        self._pipeline.wait(**kwargs)

    def stop(self):
        for process in self.controllers + self.workers + self.companions:
            process.stop()
        self._pipeline.stop()

        if self._use_vanilla_jobs and self._client is not None:
            for operation in self._client.list_operations(type="vanilla")["operations"]:
                dump_operation_stderr(
                    self._client,
                    operation["id"],
                    os.path.join(self._logs_dir, f"vanilla_{operation['id']}.err"),
                )
                if operation.get("state") not in ("completed", "failed", "aborted"):
                    try:
                        self._client.abort_operation(operation["id"])
                    except Exception as ex:
                        log.warning("Failed to abort vanilla operation %s: %s", operation["id"], ex)

    def try_dump_processes_state(self, debug_hang):
        for process in self.controllers + self.workers + self.companions:
            process.try_dump_process_state(debug_hang=debug_hang)
