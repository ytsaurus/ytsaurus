#!/usr/bin/python

import argparse
import logging
import tarfile
import os
import os.path
import shutil
import glob
import subprocess
import signal
import library.python.svn_version

logger = logging.getLogger("clickhouse-trampoline")

class SigintHandler(object):
    def __init__(self, ytserver_clickhouse_process, interrupt_child):
        self.original_sigint_handler = signal.getsignal(signal.SIGINT)
        self.sigint_index = 0
        self.ytserver_clickhouse_process = ytserver_clickhouse_process
        self.interrupt_child = interrupt_child
        logger.info("Installing custom SIGINT handler")
        signal.signal(signal.SIGINT, self.handle)

    def handle(self, signum, frame):
        assert signum == signal.SIGINT
        assert frame is not None
        if self.sigint_index == 0:
            self.sigint_index = 1
            logger.info("Caught first SIGINT")
            # NB: should not sent SIGINT to ytserver-clickhouse here as it should happen automatically in
            # YT environment.
            if self.interrupt_child:
                logger.info("Sending SIGINT to ytserver-clickhouse child, pid = %d",
                            self.ytserver_clickhouse_process.pid)
                self.ytserver_clickhouse_process.send_signal(signal.SIGINT)
                logger.info("Sent SIGINT to ytserver-clickhouse")
            else:
                logger.info("ytserver-clickhouse child should probably receive first SIGINT")
        elif self.sigint_index == 1:
            logger.info("Caught second SIGINT, restoring original SIGINT handler and interrupting ourselves")
            signal.signal(signal.SIGINT, self.original_sigint_handler)
            os.kill(os.getpid(), signal.SIGINT)
            assert False


def sigint_handler(signum, frame):
    assert signum == signal.SIGINT
    logger.info("Caught SIGINT")

    raise IOError("Couldn't open device!")


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
    # NB: without preexec_fn=os.setpgrp any signal coming to the parent will always be immediately propagated to
    # children.
    # See https://stackoverflow.com/questions/3791398/how-to-stop-python-from-propagating-signals-to-subprocesses for
    # more details.
    process = subprocess.Popen(args, preexec_fn=os.setpgrp)
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


def setup_logging():
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

def main():
    parser = argparse.ArgumentParser(description="Process that setups environment for running ytserver-clickhouse")
    parser.add_argument("--version", action="store_true", help="Print commit this binary is built from")
    parser.add_argument("ytserver_clickhouse_bin", nargs="?", help="ytserver-clickhouse binary path")
    parser.add_argument("--prepare-geodata", action="store_true", help="Extract archive with geodata")
    parser.add_argument("--monitoring-port", help="Port for monitoring HTTP server")
    parser.add_argument("--core-dump-destination", help="Path where to move all core dumps that appear after execution")
    parser.add_argument("--interrupt-child", action="store_true",
                        help="Whether child should be interrupted on first SIGINT coming to trampoline; note that it "
                             "happens automatically in YT job environment, so this option should be used only for "
                             "manual trampoline invocations")
    args = parser.parse_args()

    setup_logging()

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
    sigint_handler = SigintHandler(ytserver_clickhouse_process, args.interrupt_child)
    exit_code = ytserver_clickhouse_process.wait()
    logger.info("ytserver-clickhouse exit code is %d", exit_code)
    if args.core_dump_destination:
        move_core_dumps(args.core_dump_destination)
    exit(exit_code)


if __name__ == "__main__":
    main()
