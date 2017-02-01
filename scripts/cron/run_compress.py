#!/usr/bin/python

from yt.common import set_pdeathsig

from yt.packages.six.moves import xrange

import yt.wrapper as yt

import argparse
import subprocess

def main():
    parser = argparse.ArgumentParser(description="Script runs compression on cluster")
    parser.add_argument("--queues-root-path", required=True, help="compression queues root path")
    parser.add_argument("--max-process-count", required=True, help="maximum worker count")
    parser.add_argument("--compression-script-path", required=True)
    parser.add_argument("--log-path", required=True)

    args = parser.parse_args()

    processes = []

    total_table_count = yt.get_attribute(args.queues_root_path, "total_table_count")
    for queue in yt.list(args.queues_root_path):
        queue_size = yt.get_attribute(yt.ypath_join(args.queues_root_path, queue), "count")
        ratio = 1.0 * queue_size / total_table_count
        worker_count = int(max(1.0, ratio * args.max_process_count))
        for _ in xrange(worker_count):
            p = subprocess.Popen([
                args.compression_script_path,
                "run",
                "--queues-root-path", args.queues_root_path,
                "--queue", str(queue)
            ], preexec_fn=set_pdeathsig, stderr=open(args.log_path, "a"))

            processes.append(p)

    has_alive_processes = False
    while True:
        for p in processes:
            if p.poll() is None:
                has_alive_processes = True

        if not has_alive_processes:
            break

if __name__ == "__main__":
    main()
