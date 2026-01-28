import yt.logger as logger

from datetime import datetime
from dateutil import parser as date_parser

import argparse
import json
import os
import shutil
import subprocess
import typing

COMPONENTS = {
    "discovery": {
        "short_name": "ds",
    },
    "primaryMasters": {
        "short_name": "ms",
    },
    "masterCaches": {
        "short_name": "msc",
    },
    "httpProxies": {
        "short_name": "hp",
        "component_group_name_field": "role",
    },
    "rpcProxies": {
        "short_name": "rp",
        "component_group_name_field": "role",
    },
    "dataNodes": {
        "short_name": "dnd",
        "component_group_name_field": "name",
    },
    "execNodes": {
        "short_name": "end",
        "component_group_name_field": "name",
    },
    "tabletNodes": {
        "short_name": "tnd",
        "component_group_name_field": "name",
    },
    "queueAgents": {
        "short_name": "qa",
    },
    "tcpProxies": {
        "short_name": "tp",
        "component_group_name_field": "role",
    },
    "kafkaProxies": {
        "short_name": "kp",
        "component_group_name_field": "role",
    },
    "schedulers": {
        "short_name": "sch",
    },
    "controllerAgents": {
        "short_name": "ca",
    },
    "queryTrackers": {
        "short_name": "qt",
    },
    "yqlAgents": {
        "short_name": "yqla",
    },
    "cypressProxies": {
        "short_name": "cyp",
    },
    "bundleControllers": {
        "short_name": "bc",
    },
}


def get_logs_dir(spec: typing.Dict, component_name: str, component_group_index: int, exec_node_slot_index: typing.Optional[int]) -> str:
    component_spec = spec[component_name]
    if isinstance(component_spec, list):
        component_spec = component_spec[component_group_index]

    locations = component_spec["locations"]

    expected_location_type = "Logs"
    path_suffix = ""
    if exec_node_slot_index is not None:
        expected_location_type = "Slots"
        path_suffix = f"/{exec_node_slot_index}"

    for location in locations:
        if location["locationType"] == expected_location_type:
            return location["path"] + path_suffix

    return "/var/log"


def get_spec(kube_namespace: str, ytsaurus_name: str) -> typing.Dict:
    get_result = subprocess.check_output(
        ["kubectl", "get", "ytsaurus", "-n", kube_namespace, ytsaurus_name, "-o", "json"],
        text=True)

    return json.loads(get_result)["spec"]


def check_timestamp(from_timestamp_str: typing.Optional[str], to_timestamp_str: typing.Optional[str], modification_ts: datetime, creation_ts: datetime) -> bool:
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

    if from_timestamp_str:
        from_timestamp = datetime.strptime(from_timestamp_str, TIMESTAMP_FORMAT)
        if modification_ts < from_timestamp:
            return False

    if to_timestamp_str:
        to_timestamp = datetime.strptime(to_timestamp_str, TIMESTAMP_FORMAT)
        if creation_ts > to_timestamp:
            return False

    return True


def get_podname(component_name: str, component_group_index: int, spec: typing.Dict, pod_index: int) -> str:

    podname = f"{COMPONENTS[component_name]['short_name']}-"

    component_group_name_field = COMPONENTS[component_name].get("component_group_name_field")
    if component_group_name_field:
        assert isinstance(spec[component_name], list)
        component_group_name = spec[component_name][component_group_index].get(component_group_name_field)
        if component_group_name and component_group_name != "default":
            podname += f"{component_group_name}-"

    podname += f"{pod_index}"

    return podname


def get_filepaths(kube_namespace: str, component_name: str, component_group_index: int, exec_node_slot_index: typing.Optional[int], from_timestamp: typing.Optional[str], to_timestamp: typing.Optional[str], spec: typing.Dict, podname: str) -> typing.List[str]:
    logs_dir = get_logs_dir(spec, component_name, component_group_index, exec_node_slot_index)

    stat_result = subprocess.check_output(
        f"kubectl exec -c ytserver -n {kube_namespace} -ti {podname} -- /bin/bash -lc \"stat -c '%n %w %y' {logs_dir}/*\"",
        shell=True,
        text=True)

    filepaths = []

    for line in stat_result.split("\n"):
        parts = line.split(" ")
        if len(parts) < 7:
            continue
        filepath = parts[0]
        creation_ts_str = f"{parts[1]}T{parts[2]}{parts[3]}"
        modification_ts_str = f"{parts[4]}T{parts[5]}{parts[6]}"

        modification_ts = date_parser.parse(modification_ts_str)
        creation_ts = date_parser.parse(creation_ts_str)

        if check_timestamp(from_timestamp, to_timestamp, modification_ts, creation_ts):
            filepaths.append(filepath)

    return filepaths


def init_output(tmp_dir: str) -> None:
    os.makedirs(tmp_dir)


def finish_output(tmp_dir: str) -> str:
    archive_name = f"{tmp_dir}.tar.gz"

    subprocess.check_output(
        ["tar", "-czf", archive_name, tmp_dir],
        stderr=subprocess.DEVNULL)

    shutil.rmtree(tmp_dir)

    return archive_name


def copy_files(kube_namespace: str, current_timestamp: str, podname: str, filepaths: typing.List[str]) -> str:
    tmp_dir = f"ytsaurus_logs_{current_timestamp}_{podname}"
    init_output(tmp_dir)

    for filepath in filepaths:
        filename = filepath.split("/")[-1]
        subprocess.check_output(
            ["kubectl", "cp", "-c", "ytserver", f"{kube_namespace}/{podname}:{filepath}", f"{tmp_dir}/{filename}"],
            stdout=subprocess.DEVNULL)

    return finish_output(tmp_dir)


def grep_files(kube_namespace: str, regex: str, current_timestamp: str, podname: str, filepaths: typing.List[str]) -> str:
    tmp_dir = f"ytsaurus_logs_{current_timestamp}_{podname}"
    init_output(tmp_dir)

    for filepath in filepaths:
        filename = filepath.split("/")[-1]

        with open(f"{tmp_dir}/{filename}", "w") as fout:
            fout.write(f"Regex for zstdgrep: {regex}\n")
            fout.flush()

            subprocess.check_output(
                f"kubectl exec -c ytserver -n {kube_namespace} -ti {podname} -- /bin/bash -lc \"zstdgrep '{regex}' {filepath}\"",
                stdout=fout,
                stderr=subprocess.STDOUT,
                text=True,
                shell=True)

    return finish_output(tmp_dir)


def add_fetch_cluster_logs_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--kube-namespace", type=str, default="default")
    parser.add_argument("--ytsaurus-name", type=str, required=True)
    parser.add_argument("--from-timestamp", type=str, help="Timestamp ISO 8601 (e.g. 2025-10-28T14:30:00+03:00)")
    parser.add_argument("--to-timestamp", type=str, help="Timestamp ISO 8601 (e.g. 2025-10-28T14:30:00+03:00)")
    parser.add_argument("--component-name", type=str, choices=COMPONENTS.keys(), required=True)
    parser.add_argument("--component-group-index", type=int, default=0, help="Group index in the component spec")
    parser.add_argument("--podname", type=str, help="Specific podname")
    parser.add_argument("--exec-node-slot-index", type=int, help="Slot index for job proxy logs (usefull only for exec nodes)")
    parser.add_argument("--regex", type=str, help="Optional regex to grep with")


def fetch_cluster_logs(**kwargs) -> None:
    # Extract arguments from kwargs
    kube_namespace = kwargs.get("kube_namespace", "default")
    ytsaurus_name = kwargs.get("ytsaurus_name")
    from_timestamp = kwargs.get("from_timestamp")
    to_timestamp = kwargs.get("to_timestamp")
    component_name = kwargs.get("component_name")
    component_group_index = kwargs.get("component_group_index", 0)
    podname_arg = kwargs.get("podname")
    exec_node_slot_index = kwargs.get("exec_node_slot_index")
    regex = kwargs.get("regex")

    if exec_node_slot_index is not None and component_name != "execNodes":
        raise ValueError("--exec-node-slot-index can only be used when --component-name=execNodes")

    spec = get_spec(kube_namespace, ytsaurus_name)

    component_spec = spec[component_name]
    if isinstance(component_spec, list):
        component_spec = component_spec[component_group_index]

    current_timestamp = str(int(datetime.now().timestamp()))

    output_files = []

    for pod_index in range(component_spec["instanceCount"]):
        podname = get_podname(component_name, component_group_index, spec, pod_index)

        if podname_arg and podname != podname_arg:
            continue

        filepaths = get_filepaths(kube_namespace, component_name, component_group_index, exec_node_slot_index, from_timestamp, to_timestamp, spec, podname)

        if regex is None:
            logger.info(f"Files to copy from {podname}: {filepaths}")
            output_file = copy_files(kube_namespace, current_timestamp, podname, filepaths)
        else:
            logger.info(f"Files to grep from {podname}: {filepaths}")
            output_file = grep_files(kube_namespace, regex, current_timestamp, podname, filepaths)

        output_files.append(output_file)

    logger.info(f"Output files: {output_files}")


def add_fetch_cluster_logs_parser(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("fetch-cluster-logs", help="Fetch YT cluster logs")
    add_fetch_cluster_logs_arguments(parser)
    parser.set_defaults(func=fetch_cluster_logs)
