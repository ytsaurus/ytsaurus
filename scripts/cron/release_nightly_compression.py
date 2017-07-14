#!/usr/bin/python

from yt.wrapper.cli_helpers import run_main

import yt.logger as logger
import yt.json as json

import yt.packages.requests as requests

import yt.wrapper as yt

import os
import tarfile
import copy
import tempfile
import getpass
import subprocess

SANDBOX_API_URL = "https://sandbox.yandex-team.ru/api/v1.0/"
SANDBOX_HEADERS = {
    "Content-Type": "application/json"
}
SANDBOX_TOKEN = os.environ.get("SANDBOX_TOKEN")

FILES_TO_ARCHIVE = [
    "nightly_process_watcher.py",
    "push_nightly_process_watcher_stats.py",
    "compression/find_tables_to_compress.py",
    "compression/worker.py"
]

def sandbox_request(method, path, data=None):
    if SANDBOX_TOKEN is None:
        raise yt.YtError("Sandbox token should be specified")

    headers = copy.deepcopy(SANDBOX_HEADERS)
    headers["Authorization"] = "OAuth " + SANDBOX_TOKEN

    rsp = requests.request(method, SANDBOX_API_URL + path, headers=headers, data=data)
    rsp.raise_for_status()
    return rsp

def make_sandbox_task(archive_path):
    commit_hash = subprocess.check_output(["git", "rev-parse", "--verify", "HEAD"])
    locke_path_pattern = "http://locke.yt.yandex.net/api/v3/read_file?path={}&disposition=attachment"

    task_description = {
        "type": "REMOTE_COPY_RESOURCE",
        "context": {
            "resource_type": "YT_NIGHTLY_COMPRESSION_SCRIPTS",
            "remote_file_protocol": "http",
            "remote_file_name": locke_path_pattern.format(archive_path),
            "created_resource_name": "scripts.tar",
            "resource_attrs": "ttl=inf,backup_task=true,commit_hash={}".format(commit_hash)
        }
    }
    rsp = sandbox_request("POST", "task", data=json.dumps(task_description))
    task_id = str(rsp.json()["id"])
    logger.info("Successfully created sandbox task, id: %s", task_id)

    task_params = {
        "description": "Upload nightly compression scripts",
        "notifications": [
            {
                "transport": "email",
                "recipients": [getpass.getuser()],
                "statuses": ["SUCCESS", "FAILURE", "TIMEOUT", "EXCEPTION"]
            }
        ],
        "owner": "YT"
    }

    sandbox_request("PUT", "task/" + task_id, data=json.dumps(task_params))
    sandbox_request("PUT", "batch/tasks/start", data=json.dumps([task_id]))

    logger.info("Successfully started sandbox task %s. This task id can be used in Nanny now", task_id)

def main():
    yt.config["proxy"]["url"] = "locke"

    _, filename = tempfile.mkstemp(prefix="release_nightly_compression", suffix=".tar")
    try:
        with tarfile.TarFile(filename, mode="w") as tar:
            for rel_path in FILES_TO_ARCHIVE:
                abs_path = os.path.join(os.path.dirname(__file__), rel_path)
                tar.add(abs_path, rel_path)

        logger.info("Packaged all nightly compression files to %s", filename)

        archive_path = yt.ypath_join("//home/files", os.path.basename(filename))
        with open(filename) as tar:
            yt.write_file(archive_path, tar)

        logger.info("Successfully uploaded archive to Locke")
        make_sandbox_task(archive_path)
    finally:
        os.remove(filename)

if __name__ == "__main__":
    run_main(main)
