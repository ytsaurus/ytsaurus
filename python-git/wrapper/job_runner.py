#!/usr/bin/env python

from yt.common import makedirp
from yt.wrapper.common import MB, chunk_iter_stream
from yt.wrapper.cli_helpers import run_main

import yt.logger as logger
import yt.yson as yson

from yt.packages.six.moves import xrange
from yt.packages.six import iteritems

import yt.wrapper as yt

import os
import sys
import stat
import argparse
import subprocess
import shutil
import inspect
from functools import partial

SCRIPT_DIR = os.path.dirname(__file__)

def get_output_descriptor_list(output_table_count, use_yamr_descriptors):
    if use_yamr_descriptors:
        return [1, 2] + [i + 3 for i in xrange(output_table_count)]
    else:
        # descriptor #5 is for job statistics
        return [2, 5] + [3 * i + 1 for i in xrange(output_table_count)]

def preexec_dup2(new_fds, output_path):
    for new_fd in new_fds:
        fd = os.open(os.path.join(output_path, str(new_fd)), os.O_CREAT | os.O_WRONLY)
        os.dup2(fd, new_fd)
        if fd != new_fd:
            os.close(fd)
        if sys.version_info[:2] >= (3, 4):
            os.set_inheritable(new_fd, True)

def main():
    parser = argparse.ArgumentParser(description="Job runner for yt-job-tool")
    parser.add_argument("--config-path", help="path to YSON config")
    args = parser.parse_args()

    if args.config_path is None:
        args.config_path = os.path.join(SCRIPT_DIR, "run_config")

    with open(args.config_path, "rb") as f:
        config = yson.load(f)

    with open(config["command_path"], "r") as fin:
        command = fin.read()

    new_fds = get_output_descriptor_list(config["output_table_count"], config["use_yamr_descriptors"])

    makedirp(config["output_path"])

    env = {
        "YT_JOB_INDEX": "0",
        "YT_OPERATION_ID": config["operation_id"],
        "YT_JOB_ID": config["job_id"],
        "YT_STARTED_BY_JOB_TOOL": "1"
    }
    for var, value in iteritems(config.get("environment", {})):
        env[var] = value

    process = subprocess.Popen(command, shell=True, close_fds=False, stdin=subprocess.PIPE,
                               preexec_fn=partial(preexec_dup2, new_fds=new_fds, output_path=config["output_path"]),
                               cwd=config["sandbox_path"], env=env)
    logger.info("Started job process")

    with open(config["input_path"], "rb") as fin:
        for chunk in chunk_iter_stream(fin, 16 * MB):
            if not chunk:
                break
            process.stdin.write(chunk)
    process.stdin.close()
    process.wait()

    if process.returncode != 0:
        with open(os.path.join(config["output_path"], "2"), "rb") as f:
            stderr = f.read()
        raise yt.YtError("User job exited with non-zero exit code {0} with stderr:\n{1}"
                         .format(process.returncode, stderr))

    logger.info("Job process exited successfully. Job output (file names correspond to file descriptors) "
                "can be found in %s", config["output_path"])

def make_run_script(destination_dir):
    path = os.path.realpath(__file__)
    if path.endswith(".pyc"):
        path = path[:-1]

    destination_path = os.path.join(destination_dir, "run")
    if os.path.exists(path):
        shutil.copy2(path, destination_path)
    else:
        import library.python.resource
        with open(destination_path, "w") as f:
            f.write(library.python.resource.find("/job_runner_py_source"))

    os.chmod(destination_path, os.stat(destination_path).st_mode | stat.S_IXUSR)

if __name__ == "__main__":
    run_main(main)
