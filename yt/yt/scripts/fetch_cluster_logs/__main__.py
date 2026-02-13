from datetime import datetime
from dateutil import parser

import argparse
import json
import os
import shutil
import subprocess

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


def get_logs_dir(spec, component_name, component_group_index, exec_node_slot_index):
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


def get_spec(args):
    get_result = subprocess.check_output(
        ["kubectl", "get", "ytsaurus", "-n", args.kube_namespace, args.ytsaurus_name, "-o", "json"],
        text=True)

    return json.loads(get_result)["spec"]


def check_timestamp(args, modification_ts, creation_ts):
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"

    from_timestamp = datetime.strptime(args.from_timestamp, TIMESTAMP_FORMAT)
    to_timestamp = datetime.strptime(args.to_timestamp, TIMESTAMP_FORMAT)

    return modification_ts >= from_timestamp and creation_ts <= to_timestamp


def get_podname(args, spec, pod_index):

    podname = f"{COMPONENTS[args.component_name]["short_name"]}-"

    component_group_name_field = COMPONENTS[args.component_name].get("component_group_name_field")
    if component_group_name_field:
        assert isinstance(spec[args.component_name], list)
        component_group_name = spec[args.component_name][args.component_group_index].get(component_group_name_field)
        if component_group_name and component_group_name != "default":
            podname += f"{component_group_name}-"

    podname += f"{pod_index}"

    return podname


def get_filepaths(args, spec, podname):
    logs_dir = get_logs_dir(spec, args.component_name, args.component_group_index, args.exec_node_slot_index)

    stat_result = subprocess.check_output(
        f"kubectl exec -c ytserver -n {args.kube_namespace} -ti {podname} -- /bin/bash -lc \"stat -c '%n %w %y' {logs_dir}/*\"",
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

        modification_ts = parser.parse(modification_ts_str)
        creation_ts = parser.parse(creation_ts_str)

        if check_timestamp(args, modification_ts, creation_ts):
            filepaths.append(filepath)

    return filepaths


def init_output(tmp_dir):
    os.makedirs(tmp_dir)


def finish_output(tmp_dir):
    archive_name = f"{tmp_dir}.tar.gz"

    subprocess.check_output(
        ["tar", "-czf", archive_name, tmp_dir],
        stderr=subprocess.DEVNULL)

    shutil.rmtree(tmp_dir)

    return archive_name


def copy_files(args, current_timestamp, podname, filepaths):
    tmp_dir = f"ytsaurus_logs_{current_timestamp}_{podname}"
    init_output(tmp_dir)

    for filepath in filepaths:
        subprocess.check_output(
            ["kubectl", "cp", "-c", "ytserver", f"{args.kube_namespace}/{podname}:{filepath}", "{tmp_dir}/{filepath.split("/")[-1]}"],
            stdout=subprocess.DEVNULL)

    return finish_output(tmp_dir)


def grep_files(args, current_timestamp, podname, filepaths):
    tmp_dir = f"ytsaurus_logs_{current_timestamp}_{podname}"
    init_output(tmp_dir)

    for filepath in filepaths:
        filename = filepath.split("/")[-1]

        with open(f"{tmp_dir}/{filename}", "w") as fout:
            fout.write(f"Regex for zstdgrep: {args.regex}\n")
            fout.flush()

            subprocess.check_output(
                f"kubectl exec -c ytserver -n {args.kube_namespace} -ti {podname} -- /bin/bash -lc \"zstdgrep '{args.regex}' {filepath}\"",
                stdout=fout,
                stderr=subprocess.STDOUT,
                text=True,
                shell=True)

    return finish_output(tmp_dir)


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch YT cluster logs")
    parser.add_argument("--kube-namespace", type=str, default="default")
    parser.add_argument("--ytsaurus-name", type=str, required=True)
    parser.add_argument("--from-timestamp", type=str, help="Timestamp ISO 8601 (e.g. 2025-10-28T14:30:00+03:00)")
    parser.add_argument("--to-timestamp", type=str, help="Timestamp ISO 8601 (e.g. 2025-10-28T14:30:00+03:00)")
    parser.add_argument("--component-name", type=str, choices=COMPONENTS.keys(), required=True)
    parser.add_argument("--component-group-index", type=int, default=0, help="Group index in the component spec")
    parser.add_argument("--podname", type=str, help="Specific podname")
    parser.add_argument("--exec-node-slot-index", type=int, help="Slot index for job proxy logs (usefull only for exec nodes)")
    parser.add_argument("--regex", type=str, help="Optional regex to grep with")

    args = parser.parse_args()

    if args.exec_node_slot_index is not None and args.component_name != "execNodes":
        parser.error("--exec-node-slot-index can only be used when --component-name=execNodes")

    return args


def main():
    args = parse_args()

    spec = get_spec(args)

    component_spec = spec[args.component_name]
    if isinstance(component_spec, list):
        component_spec = component_spec[args.component_group_index]

    current_timestamp = str(int(datetime.now().timestamp()))

    output_files = []

    for pod_index in range(component_spec["instanceCount"]):
        podname = get_podname(args, spec, pod_index)

        if args.podname and podname != args.podname:
            continue

        filepaths = get_filepaths(args, spec, podname)

        if args.regex is None:
            print(f"Files to copy from {podname}: ", filepaths)
            output_file = copy_files(args, current_timestamp, podname, filepaths)
        else:
            print(f"Files to grep from {podname}: ", filepaths)
            output_file = grep_files(args, current_timestamp, podname, filepaths)

        output_files.append(output_file)

    print(f"Output files: {output_files}")


if __name__ == "__main__":
    main()
