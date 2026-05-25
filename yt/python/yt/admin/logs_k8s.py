import yt.logger as logger
import yt.yson as yson
from yt.admin._experimental import warn_experimental, EXPERIMENTAL_HELP_SUFFIX

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Set
import argparse
import os
import re
import shlex


COMPONENTS_MAP = {
    "discovery": {"short_name": "ds", "crd_name": "discovery", "cfg_name": "discovery"},
    "primary_masters": {"short_name": "ms", "crd_name": "primaryMasters", "cfg_name": "master"},
    "master_caches": {"short_name": "msc", "crd_name": "masterCaches", "cfg_name": "master-cache"},
    "http_proxies": {"short_name": "hp", "crd_name": "httpProxies", "cfg_name": "http-proxy", "group_field": "role"},
    "rpc_proxies": {"short_name": "rp", "crd_name": "rpcProxies", "cfg_name": "rpc-proxy", "group_field": "role"},
    "data_nodes": {"short_name": "dnd", "crd_name": "dataNodes", "cfg_name": "data-node", "group_field": "name"},
    "exec_nodes": {"short_name": "end", "crd_name": "execNodes", "cfg_name": "exec-node", "group_field": "name"},
    "tablet_nodes": {"short_name": "tnd", "crd_name": "tabletNodes", "cfg_name": "tablet-node", "group_field": "name"},
    "queue_agents": {"short_name": "qa", "crd_name": "queueAgents", "cfg_name": "queue-agent"},
    "tcp_proxies": {"short_name": "tp", "crd_name": "tcpProxies", "cfg_name": "tcp-proxy", "group_field": "role"},
    "kafka_proxies": {"short_name": "kp", "crd_name": "kafkaProxies", "cfg_name": "kafka-proxy", "group_field": "role"},
    "schedulers": {"short_name": "sch", "crd_name": "schedulers", "cfg_name": "scheduler"},
    "controller_agents": {"short_name": "ca", "crd_name": "controllerAgents", "cfg_name": "controller-agent"},
    "query_trackers": {"short_name": "qt", "crd_name": "queryTrackers", "cfg_name": "query-tracker"},
    "yql_agents": {"short_name": "yqla", "crd_name": "yqlAgents", "cfg_name": "yql-agent"},
    "cypress_proxies": {"short_name": "cyp", "crd_name": "cypressProxies", "cfg_name": "cypress-proxy"},
    "bundle_controllers": {"short_name": "bc", "crd_name": "bundleController", "cfg_name": "bundle-controller"},
}

YTSERVER_CONTAINER = "ytserver"


def _parse_iso8601(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z") or normalized.endswith("z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)


@dataclass
class LogFetchConfig:
    component_name: str
    group_name: str
    pods: Optional[List[str]]
    exec_slot_index: Optional[int]
    writer: Optional[List[str]]
    writer_force: Optional[List[str]]
    from_ts: Optional[datetime]
    to_ts: Optional[datetime]
    output: str
    grep: Optional[str]
    yes: bool


@dataclass
class RemoteFile:
    pod_name: str
    container: str
    path: str
    size: int
    mtime: datetime
    ctime: datetime
    inode: int

    @property
    def name(self) -> str:
        return os.path.basename(self.path)


def _k8s_install_hint() -> str:
    from importlib.metadata import packages_distributions
    dists = packages_distributions().get("yt", [])
    for name in ("ytsaurus-client", "yandex-yt"):
        if name in dists:
            return f"pip install '{name}[admin]'"
    return "pip install kubernetes"


class K8sExecutor:
    def __init__(self, namespace: str):
        try:
            from kubernetes import client, config, stream
        except ImportError as e:
            raise ImportError(
                "The 'kubernetes' package is required for K8sExecutor. "
                f"Install it with: {_k8s_install_hint()}"
            ) from e

        self.namespace = namespace
        try:
            config.load_kube_config()
        except config.ConfigException:
            config.load_incluster_config()

        self._v1_api = client.CoreV1Api()
        self._custom_objects_api = client.CustomObjectsApi()
        self._stream = stream.stream

    def get_cluster_spec(self, cluster_name: str) -> Dict[str, Any]:
        return self._custom_objects_api.get_namespaced_custom_object(
            group="cluster.ytsaurus.tech",
            version="v1",
            plural="ytsaurus",
            namespace=self.namespace,
            name=cluster_name,
        )

    def stream_exec(self, pod_name: str, cmd: List[str]) -> Generator[bytes, None, None]:
        response = self._stream(
            self._v1_api.connect_get_namespaced_pod_exec,
            pod_name,
            self.namespace,
            command=cmd,
            container=YTSERVER_CONTAINER,
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
            binary=True,
            _preload_content=False,
        )

        if TYPE_CHECKING:
            from kubernetes.stream.ws_client import WSClient

            assert isinstance(response, WSClient)

        while response.is_open():
            response.update(timeout=1)
            if response.peek_stderr():
                logger.warning(f"Stderr from pod {pod_name}: {response.read_stderr()}")
            if response.peek_stdout():
                yield response.read_stdout()
        response.close()
        if response.returncode != 0:
            raise RuntimeError(f"Command '{cmd}' failed with code {response.returncode}")

    def get_file_content(self, pod_name: str, path: str) -> bytes:
        return b"".join(self.stream_exec(pod_name, ["cat", path]))


@dataclass
class YtComponentGroup:
    component_type: str
    group_name: str
    replicas: int
    locations: List[Dict[str, Any]]


class YtClusterManager:
    def __init__(self, k8s: K8sExecutor, cluster_name: str):
        self.k8s = k8s
        self.cluster_name = cluster_name
        self._spec = None

    @property
    def spec(self):
        if not self._spec:
            self._spec = self.k8s.get_cluster_spec(self.cluster_name)
        return self._spec

    def get_group_info(self, component_type: str, group_name: str) -> YtComponentGroup:
        component_meta = COMPONENTS_MAP.get(component_type)

        component_crd_name = component_meta.get("crd_name")
        component_group_field = component_meta.get("group_field")
        component_spec = self.spec.get("spec", {}).get(component_crd_name)

        if not component_spec:
            raise ValueError(f"No configuration found for {component_crd_name} in cluster spec")

        component_instance_groups: List[Dict[str, Any]] = []
        if isinstance(component_spec, list):
            component_instance_groups = component_spec
        else:
            component_instance_groups = [component_spec]

        target_component_instance_group = None
        for component_instance_group in component_instance_groups:
            actual_group = component_instance_group.get(component_group_field, "default") if component_group_field else "default"
            if actual_group == group_name:
                target_component_instance_group = component_instance_group
                break

        if not target_component_instance_group:
            raise ValueError(f"Group '{group_name}' not found for component '{component_type}'")

        return YtComponentGroup(
            component_type=component_type,
            group_name=group_name,
            replicas=target_component_instance_group.get("instanceCount", 0),
            locations=target_component_instance_group.get("locations", []),
        )

    def get_pod_names(self, info: YtComponentGroup) -> List[str]:
        pods = []
        component_meta = COMPONENTS_MAP.get(info.component_type)
        for i in range(info.replicas):
            suffix = f"-{info.group_name}-{i}" if info.group_name != "default" else f"-{i}"
            pods.append(f"{component_meta.get('short_name')}{suffix}")
        return pods

    def get_log_dir(self, info: YtComponentGroup, slot_index: Optional[int]) -> str:
        target_type = "Slots" if slot_index is not None else "Logs"
        path_suffix = f"/{slot_index}" if slot_index is not None else ""

        for location in info.locations:
            if location.get("locationType") == target_type:
                return location["path"] + path_suffix
        return "/var/log"

    def get_config_path(self, info: YtComponentGroup, config_dir: str, slot_index: Optional[int]) -> str:
        if slot_index is not None:
            return os.path.join(config_dir, "config.yson")

        config_file_name_suffix = COMPONENTS_MAP[info.component_type]["cfg_name"]
        return f"/config/ytserver-{config_file_name_suffix}.yson"


class LogDownloader:
    def __init__(self, k8s: K8sExecutor, yt: YtClusterManager):
        self.k8s = k8s
        self.yt = yt
        self._overwrite_all = False
        self._skip_all = False

    def list_files(self, pod: str, directory: str) -> List[RemoteFile]:
        cmd = ["/bin/bash", "-c", f"stat -c '%F|%Y|%W|%s|%i|%n' {directory}/* 2>/dev/null"]
        raw_gen = self.k8s.stream_exec(pod, cmd)
        lines = b"".join(raw_gen).decode("utf-8", errors="ignore").splitlines()
        files: List[RemoteFile] = []
        for line in lines:
            parts = line.split("|", 5)
            if len(parts) != 6:
                continue
            file_type, mtime_ts_str, ctime_ts_str, size_str, file_inode_str, file_path = parts
            if "regular file" not in file_type.lower():
                continue
            mtime = datetime.fromtimestamp(int(mtime_ts_str), timezone.utc)
            ctime_ts = int(ctime_ts_str) if ctime_ts_str.isdigit() and int(ctime_ts_str) > 0 else int(mtime_ts_str)
            ctime = datetime.fromtimestamp(ctime_ts, timezone.utc)
            file_inode = int(file_inode_str)
            files.append(RemoteFile(pod, YTSERVER_CONTAINER, file_path, int(size_str), mtime, ctime, file_inode))
        return files

    def get_filter_patterns(self, pod_name: str, config_path: str, writers: Optional[List[str]]) -> Set[str]:
        patterns: Set[str] = set()
        content = self.k8s.get_file_content(pod_name, config_path)
        cfg: Dict[str, Any] = yson.loads(content)
        writers_cfg: Dict[str, Any] = cfg.get("logging", {}).get("writers", {})
        writers_to_remove = []
        for writer_name, writer_cfg in writers_cfg.items():
            if writer_cfg["type"] == "file":
                continue
            writers_to_remove.append(writer_name)
        for writer_to_remove in writers_to_remove:
            del writers_cfg[writer_to_remove]
        logger.debug("Available writers %s, requested writers %s.", list(writers_cfg.keys()), writers)

        for writer in writers:
            if writer in writers_cfg:
                if "file_name" not in writers_cfg[writer]:
                    raise ValueError(f"Writer '{writer}' not supported.")
                file_name: str = os.path.basename(writers_cfg[writer]["file_name"])
                patterns.add(r"^" + re.escape(file_name))
            else:
                raise ValueError(f"Writer '{writer}' not found in config.")
        return patterns

    def _get_target_pods(self, group_info: YtComponentGroup, pods_filter: Optional[List[str]]) -> List[str]:
        all_pods = self.yt.get_pod_names(group_info)
        pods = [p for p in all_pods if p in pods_filter] if pods_filter else all_pods
        if not pods:
            logger.error("No pods found.")
            return []
        logger.debug("Fetching logs from pods %s", pods)
        return pods

    def _get_log_patterns(self, group_info: YtComponentGroup, config_dir: str, pod: str, cfg: LogFetchConfig) -> Set[str]:
        patterns: Set[str] = set()
        if cfg.writer_force:
            patterns.update(cfg.writer_force)
        if cfg.writer:
            config_path = self.yt.get_config_path(group_info, config_dir, cfg.exec_slot_index)
            logger.debug(f"Reading config from {config_path}")
            patterns.update(self.get_filter_patterns(pod, config_path, cfg.writer))
        if patterns:
            logger.debug("Fetching logs that matches any of regexes %s", patterns)
        return patterns

    def _collect_logs_from_pods(self, pods: List[str], log_dir: str, patterns: Set[str], cfg: LogFetchConfig) -> Dict[str, List[RemoteFile]]:
        files_map: Dict[str, List[RemoteFile]] = {}
        total_size = 0

        for pod in pods:
            filtered_log_files: List[RemoteFile] = []
            pod_log_size = 0
            log_files = self.list_files(pod, log_dir)
            for filtered_log_file in log_files:
                if len(filtered_log_file.name.split(".")) < 3:
                    continue
                if filtered_log_file.name.split(".")[2] != "log":
                    continue
                if patterns:
                    if not any(re.search(p, filtered_log_file.name) for p in patterns):
                        continue
                if cfg.from_ts and filtered_log_file.mtime < cfg.from_ts.astimezone(timezone.utc):
                    continue
                if cfg.to_ts and filtered_log_file.ctime > cfg.to_ts.astimezone(timezone.utc):
                    continue
                pod_log_size += filtered_log_file.size
                filtered_log_files.append(filtered_log_file)
                logger.debug(
                    f"Found on pod {filtered_log_file.pod_name} log file {filtered_log_file.path}, size {filtered_log_file.size}, ctime {filtered_log_file.ctime}, mtime {filtered_log_file.mtime}"
                )

            if filtered_log_files:
                files_map[pod] = filtered_log_files
            total_size += pod_log_size
            self._log_statistics(pod, pod_log_size, filtered_log_files)

        if files_map:
            logger.info(f"Found {sum(len(v) for v in files_map.values())} files. Size: {total_size / (2 ** 20):.2f} MB")
        return files_map

    def run(self, cfg: LogFetchConfig):
        group_info = self.yt.get_group_info(cfg.component_name, cfg.group_name)
        logger.debug(f"Fetching logs from {group_info.component_type} (group {group_info.group_name})")

        pods = self._get_target_pods(group_info, cfg.pods)
        if not pods:
            return

        log_dir = self.yt.get_log_dir(group_info, cfg.exec_slot_index)
        logger.debug("Fetching logs from dir %s", log_dir)

        patterns = self._get_log_patterns(group_info, log_dir, pods[0], cfg)

        files_map = self._collect_logs_from_pods(pods, log_dir, patterns, cfg)

        if not files_map:
            logger.info("No logs matched criteria.")
            return

        if not cfg.yes:
            if input("Download? [y/N]: ").lower() not in ("y", "yes"):
                return
        self._download_logs(files_map, cfg.output, cfg.exec_slot_index, cfg.grep)

    def _log_statistics(self, pod_name: str, pod_log_size: int, filtered_log_files: List[RemoteFile]) -> None:
        logger.info(f"Pod {pod_name}: Found {len(filtered_log_files)} files. Approx size: {pod_log_size / (2 ** 20):.2f} MB.")
        stats: Dict[str, Dict[str, Any]] = {}
        for filtered_log_file in filtered_log_files:
            writer_type = filtered_log_file.name.split(".")[1]
            if writer_type not in stats:
                stats[writer_type] = {"count": 0, "size": 0, "min_ctime": filtered_log_file.ctime, "max_mtime": filtered_log_file.mtime}
            stats[writer_type]["count"] += 1
            stats[writer_type]["size"] += filtered_log_file.size
            stats[writer_type]["min_ctime"] = min(stats[writer_type]["min_ctime"], filtered_log_file.ctime)
            stats[writer_type]["max_mtime"] = max(stats[writer_type]["max_mtime"], filtered_log_file.mtime)
        for log_type, data in sorted(stats.items()):
            size_mb = data["size"] / (2 ** 20)
            logger.info(f"- {log_type}: {data['count']} files, {size_mb:.2f} MB, from {data['min_ctime']} to {data['max_mtime']}")

    def _resolve_conflict(self, full_path: str) -> bool:
        if self._overwrite_all:
            return True
        if self._skip_all:
            return False

        while True:
            choice = input(f"File {full_path} already exists. [r]eplace / [R]eplace all / [s]kip / [S]kip all? ")
            if choice == "r":
                return True
            elif choice == "R":
                self._overwrite_all = True
                return True
            elif choice == "s":
                return False
            elif choice == "S":
                self._skip_all = True
                return False

    def _prepare_download_path(self, output_path: str, pod: str, file: RemoteFile, slot_index: Optional[int], grep_pattern: Optional[str]) -> Optional[str]:
        rel_file_path = f"{pod}/{file.name}"
        if slot_index is not None:
            rel_file_path = f"{pod}/slot/{slot_index}/{file.name}"

        if grep_pattern:
            if ".zstd" in file.name:
                rel_file_path = rel_file_path.replace(".zstd", "")
            elif ".gz" in file.name:
                rel_file_path = rel_file_path.replace(".gz", "")

        full_path = os.path.join(output_path, rel_file_path)
        if os.path.exists(full_path):
            if not self._resolve_conflict(full_path):
                return None

        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        return full_path

    def _generate_download_command(self, file: RemoteFile, search_dir: str, grep_pattern: Optional[str]) -> List[str]:
        echo_if_eq_inode = f"""
import os, sys
inode_target = {file.inode}
path = sys.argv[1]
fd = os.open(path, os.O_RDONLY)
stat = os.fstat(fd)
if stat.st_ino == inode_target:
    with os.fdopen(fd, "rb") as f:
        while True:
            chunk = f.read(1024*1024)
            if not chunk:
                break
            sys.stdout.buffer.write(chunk)
else:
    print(f"Target inode {{inode_target}} dont match with file inode {{stat.st_ino}}", file=sys.stderr)
    os.close(fd)
    sys.exit(1)
"""
        exec_cmd = []
        if grep_pattern:
            decompress_pipe = ""
            if ".zstd" in file.name:
                decompress_pipe = "| zstd -dcf"
            elif ".gz" in file.name:
                decompress_pipe = "| gzip -dcf"

            quoted_pattern = shlex.quote(grep_pattern)
            bash_pipeline = f'python3 -c "$1" "$2" {decompress_pipe} | grep -a -- {quoted_pattern}'

            exec_cmd = ["bash", "-c", bash_pipeline, "--", echo_if_eq_inode, "{}"]
        else:
            exec_cmd = ["python3", "-c", echo_if_eq_inode, "{}"]

        return ["find", search_dir, "-maxdepth", "1", "-inum", str(file.inode), "-exec", *exec_cmd, ";"]

    def _execute_download(self, pod: str, cmd: List[str], full_path: str, grep_pattern: Optional[str]) -> bool:
        stream_gen = self.k8s.stream_exec(pod, cmd)
        written_bytes = 0
        with open(full_path, "wb") as f:
            for chunk in stream_gen:
                f.write(chunk)
                written_bytes += len(chunk)

        if grep_pattern and written_bytes == 0:
            os.remove(full_path)
            return False
        return True

    def _download_logs(self, files_map: Dict[str, List[RemoteFile]], output_path: str, slot_index: Optional[int], grep_pattern: Optional[str] = None):
        os.makedirs(output_path, exist_ok=True)

        for pod, files in files_map.items():
            search_dir = os.path.dirname(files[0].path)
            for file in files:
                logger.debug(f"Downloading {pod}/{file.name} (inode {file.inode}, orig_size {file.size})")

                full_path = self._prepare_download_path(output_path, pod, file, slot_index, grep_pattern)
                if full_path is None:
                    logger.info(f"Skipping {pod}/{file.name}")
                    continue

                cmd = self._generate_download_command(file, search_dir, grep_pattern)

                if self._execute_download(pod, cmd, full_path, grep_pattern):
                    logger.info(f"File {pod}/{file.name} downloaded to '{full_path}'.")


def _add_logs_k8s_arguments(parser) -> None:
    current_time = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    parser.add_argument("-n", "--namespace", type=str, default="default", help="Kubernetes namespace")
    parser.add_argument("cluster_name", type=str, help="YTsaurus cluster name (see: kubectl get ytsaurus)")
    parser.add_argument("component_name", type=str, choices=COMPONENTS_MAP.keys(), help="Component name")
    parser.add_argument("group_name", nargs="?", default="default", help="Component group name")
    parser.add_argument("-p", "--pods", action="append", type=str, help="Specific pod name (can be specified multiple times)")
    parser.add_argument("--exec-slot-index", type=int, help="Slot index for job proxy logs (useful only for exec nodes)")
    parser.add_argument("--from-ts", type=str, metavar="ISO8601", help=f"Filter logs from this time (i.e. {current_time})")
    parser.add_argument("--to-ts", type=str, metavar="ISO8601", help=f"Filter logs up to this time (i.e. {current_time})")
    parser.add_argument("-w", "--writer", type=str, action="append", help="Filter by writer name (can be specified multiple times)")
    parser.add_argument("--writer-force", type=str, metavar="regex", action="append", help="Force add regex pattern for filename filtering")
    parser.add_argument("-o", "--output", type=str, metavar="dir", default="logs", help="Output directory path")
    parser.add_argument("--grep", type=str, metavar="regex", help="Filter logs content by regex")
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")


@warn_experimental
def run_logs_k8s(namespace, cluster_name, component_name, group_name, pods, exec_slot_index, from_ts, to_ts, writer, writer_force, output, grep, yes, **_) -> None:
    if exec_slot_index is not None and component_name != "exec_nodes":
        raise ValueError("--exec-slot-index can only be used when component_name=exec_nodes")

    cfg = LogFetchConfig(
        component_name=component_name,
        group_name=group_name,
        pods=pods,
        exec_slot_index=exec_slot_index,
        writer=writer,
        writer_force=writer_force,
        from_ts=_parse_iso8601(from_ts) if from_ts else None,
        to_ts=_parse_iso8601(to_ts) if to_ts else None,
        output=output,
        grep=grep,
        yes=yes,
    )

    k8s = K8sExecutor(namespace)
    cluster_manager = YtClusterManager(k8s, cluster_name)
    app = LogDownloader(k8s, cluster_manager)
    app.run(cfg)


def add_logs_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "logs",
        help="Fetch cluster logs",
        description="Fetch cluster logs. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    logs_subparsers = parser.add_subparsers()
    logs_subparsers.required = True

    k8s_parser = logs_subparsers.add_parser(
        "k8s",
        help="Fetch YT cluster logs via Kubernetes API",
        description="Fetch YT cluster logs via Kubernetes API. " + EXPERIMENTAL_HELP_SUFFIX,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    k8s_parser.set_defaults(func=run_logs_k8s)
    _add_logs_k8s_arguments(k8s_parser)
