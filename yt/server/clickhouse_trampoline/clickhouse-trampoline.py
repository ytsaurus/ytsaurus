#!/usr/bin/python

import argparse
import logging
import tarfile
import os
import os.path
import shutil
import glob
import subprocess
import library.python.svn_version

logger = logging.getLogger("clickhouse-trampoline")


def extract_geodata():
    logger.info("Extracting geodata")
    assert os.path.exists("./geodata.tgz")
    tar = tarfile.open("./geodata.tgz", "r:gz")
    os.mkdir("./geodata")
    tar.extractall(path="./geodata/")
    tar.close()
    logger.info("Geodata extracted")


def patch_config(prepare_geodata):
    logger.info("Patching config")
    assert os.path.exists("./config.yson")
    with open("./config.yson", "r") as f:
        content = f.read()
    content = content.replace("$YT_JOB_ID", os.environ["YT_JOB_ID"])
    if not prepare_geodata:
        content = "\n".join(filter(lambda line: "./geodata" not in line, content.split("\n")))
    with open("./config_patched.yson", "w") as f:
        f.write(content)
    logger.info("Config patched")


def run_ytserver_clickhouse(ytserver_clickhouse_bin, monitoring_port):
    logger.info("Starting ytserver-clickhouse")
    args = [ytserver_clickhouse_bin]
    args += ["--config", "./config_patched.yson"]
    args += ["--instance-id", os.environ["YT_JOB_ID"]]
    args += ["--clique-id", os.environ["YT_OPERATION_ID"]]
    args += ["--rpc-port", os.environ["YT_PORT_0"]]
    args += ["--monitoring-port", monitoring_port or os.environ["YT_PORT_1"]]
    args += ["--tcp-port", os.environ["YT_PORT_2"]]
    args += ["--http-port", os.environ["YT_PORT_3"]]
    logger.info("Going to invoke following command: %s", args)
    process = subprocess.Popen(args)
    logger.info("Process started, pid = %d", process.pid)
    return process


def move_core_dumps(destination):
    logger.info("Moving core dumps to %s", destination)
    for file in glob.glob("./core*"):
        logger.debug("Moving %s to %s", file, destination)
        shutil.copy2(file, destination)
        logger.debug("%s moved")
    logger.info("Core dumps moved")


def print_version():
    print "smth~" + library.python.svn_version.commit_id()


def main():
    logging.basicConfig(format="%(asctime)s %(levelname)s\t%(message)s", level=logging.DEBUG)

    parser = argparse.ArgumentParser(description="Process that setups environment for running ytserver-clickhouse")
    parser.add_argument("--version", action="store_true", help="Print commit this binary is built from")
    parser.add_argument("ytserver_clickhouse_bin", nargs="?", help="ytserver-clickhouse binary path")
    parser.add_argument("--prepare-geodata", action="store_true", help="Extract archive with geodata")
    parser.add_argument("--monitoring-port", help="Port for monitoring HTTP server")
    parser.add_argument("--core-dump-destination", help="Path where to move all core dumps that appear after execution")
    args = parser.parse_args()

    logger.info("Trampoline started, args = %s", args)
    if args.version:
        print_version()
        exit(0)

    if not args.ytserver_clickhouse_bin:
        parser.error("Only positional argument 'ytserver_clickhouse_bin' should be present")
        exit(1)

    if args.prepare_geodata:
        extract_geodata()
    patch_config(args.prepare_geodata)
    ytserver_clickhouse_process = run_ytserver_clickhouse(args.ytserver_clickhouse_bin, args.monitoring_port)
    exit_code = ytserver_clickhouse_process.wait()
    logger.info("ytserver-clickhouse exit code is %d", exit_code)
    if args.core_dump_destination:
        move_core_dumps(args.core_dump_destination)
    exit(exit_code)


if __name__ == "__main__":
    main()
