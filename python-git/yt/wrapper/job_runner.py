#!/usr/bin/env python

from __future__ import print_function

# NOTE(asaitgalin): This script should not depend on YT Python library.

import os
import sys
import stat
import json
import errno
import argparse
import subprocess
import shutil
from functools import partial

PY3 = sys.version_info[0] == 3
SCRIPT_DIR = os.path.dirname(__file__)

if PY3:
    def iteritems(d):
        return iter(d.items())
else:
    def iteritems(d):
        return d.iteritems()

def chunk_iter_stream(stream, chunk_size):
    while True:
        chunk = stream.read(chunk_size)
        if not chunk:
            break
        yield chunk

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
    parser.add_argument("--config-path", help="path to JSON config")
    args = parser.parse_args()

    if args.config_path is None:
        args.config_path = os.path.join(SCRIPT_DIR, "run_config")

    with open(args.config_path, "rb") as f:
        config = json.load(f)

    with open(config["command_path"], "r") as fin:
        command = fin.read()

    new_fds = get_output_descriptor_list(config["output_table_count"], config["use_yamr_descriptors"])

    try:
        os.makedirs(config["output_path"])
    except OSError as err:
        if err.errno != errno.EEXIST:
            raise

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

    with open(config["input_path"], "rb") as fin:
        for chunk in chunk_iter_stream(fin, 16 * 1024 * 1024):
            if not chunk:
                break
            process.stdin.write(chunk)
    process.stdin.close()
    process.wait()

    if process.returncode != 0:
        with open(os.path.join(config["output_path"], "2"), "rb") as f:
            print("User job exited with non-zero exit code {} and stderr:\n{}".format(process.returncode, f.read()),
                  file=sys.stderr)
            sys.exit(1)

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
    main()
