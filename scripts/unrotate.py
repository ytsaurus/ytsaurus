#!/usr/bin/python3
import os
import sys
import argparse
import glob
import subprocess


parser = argparse.ArgumentParser(description='Rotate logs backwards.')
parser.add_argument('--test-runner', choices=['dist', 'legacy'], default='dist')
parser.add_argument('--dir', type=str, required=False)


def unrotate(log_dir):
    groups = {}
    for log in os.listdir(log_dir):
        name = log[:log.find(".log")]
        groups.setdefault(name, []).append(log)

    for name, logs in groups.items():
        tmp_name = os.path.join(log_dir, name + ".tmp")

        def log_index(name):
            if name.endswith(".log"):
                return 0

            return int(name.split(".")[-2])

        for log in sorted(logs, reverse=True, key=log_index):
            log_name = os.path.join(log_dir, log)
            stat = os.stat(log_name)
            if stat.st_size == 2 * 1024 * 1024:
                sys.stderr.write("WARNING: {} is truncated by the distbuild\n".format(log_name))

            cat = None
            if log.endswith(".gz"):
                cat = "zcat"
            else:
                cat = "cat"

            subprocess.call("{} {} >> {}".format(cat, log_name, tmp_name), shell=True)
            os.unlink(log_name)

        os.rename(tmp_name, os.path.join(log_dir, name + ".log"))


def main():
    args = parser.parse_args()

    if args.dir is not None:
        log_dirs = [args.dir]
    elif args.test_runner == "dist":
        dir = "yt/tests/integration/tests/test-results/yt-tests-integration-tests/testing_out_stuff"
        log_dirs = glob.glob(dir + "/*/logs")
    else:
        dir = "yt/tests/integration/tests.sandbox"
        log_dirs = glob.glob(dir + "/*/run_latest/logs")

    for log_dir in log_dirs:
        unrotate(log_dir)


if __name__ == "__main__":
    main()
