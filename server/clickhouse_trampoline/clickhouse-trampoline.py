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


def substitute_env(content):
    content = content.replace("$YT_JOB_INDEX", os.environ["YT_JOB_INDEX"])
    content = content.replace("$YT_JOB_COOKIE", os.environ["YT_JOB_COOKIE"])
    return content


def patch_ytserver_clickhouse_config(prepare_geodata):
    logger.info("Patching config")
    assert os.path.exists("./config.yson")
    with open("./config.yson", "r") as f:
        content = f.read()
    content = substitute_env(content)
    if not prepare_geodata:
        content = "\n".join(filter(lambda line: "./geodata" not in line, content.split("\n")))
    with open("./config_patched.yson", "w") as f:
        f.write(content)
    logger.info("Config patched")


def patch_log_tailer_config():
    logger.info("Patching log tailer config")
    assert os.path.exists("./log_tailer_config.yson")
    with open("./log_tailer_config.yson", "r") as f:
        content = f.read()
    content = substitute_env(content)
    with open("./log_tailer_config_patched.yson", "w") as f:
        f.write(content)
    logger.info("Config patched")


def start_process(args):
    logger.info("Going to invoke following command: %s", args)
    # NB: without preexec_fn=os.setpgrp any signal coming to the parent will always be immediately propagated to
    # children.
    # See https://stackoverflow.com/questions/3791398/how-to-stop-python-from-propagating-signals-to-subprocesses for
    # more details.
    process = subprocess.Popen(args, preexec_fn=os.setpgrp)
    logger.info("Process started, pid = %d", process.pid)
    return process


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
    args += ["--pdeathsig", "9"]
    return start_process(args)


def run_log_tailer(log_tailer_bin, ytserver_clickhouse_pid, monitoring_port):
    logger.info("Running log tailer over pid %d", ytserver_clickhouse_pid)
    args = [log_tailer_bin, str(ytserver_clickhouse_pid), "--config", "./log_tailer_config_patched.yson"]
    args += ["--monitoring-port", monitoring_port or os.environ["YT_PORT_4"]]
    return start_process(args)


def move_core_dumps(destination):
    logger.info("Hardlinking core dumps to %s", destination)
    for file in glob.glob("./core*"):
        logger.debug("Hardlinking %s to point to %s", destination, file)
        logger.debug("Core %s size is %s", file, os.stat(file).st_size)
        destination_file = os.path.join(destination, file)
        os.link(file, destination_file)
        logger.debug("%s moved", file)
    logger.info("Core dumps hardlinked")


def print_version():
    print "smth~" + library.python.svn_version.commit_id()[:10]


def setup_logging(log_file):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    file_handler = logging.FileHandler(log_file or "trampoline.debug.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def main():
    parser = argparse.ArgumentParser(description="Process that setups environment for running ytserver-clickhouse")
    parser.add_argument("--version", action="store_true", help="Print commit this binary is built from")
    parser.add_argument("ytserver_clickhouse_bin", nargs="?", help="ytserver-clickhouse binary path")
    parser.add_argument("--prepare-geodata", action="store_true", help="Extract archive with geodata")
    parser.add_argument("--monitoring-port", help="Port for monitoring HTTP server")
    parser.add_argument("--log-tailer-monitoring-port", help="Port for log tailer monitoring HTTP server")
    parser.add_argument("--core-dump-destination", help="Path where to move all core dumps that appear after execution")
    parser.add_argument("--interrupt-child", action="store_true",
                        help="Whether child should be interrupted on first SIGINT coming to trampoline; note that it "
                             "happens automatically in YT job environment, so this option should be used only for "
                             "manual trampoline invocations")
    parser.add_argument("--log-tailer-bin", help="Log tailer binary path; log tailer will be run over "
                                                 "ytserver-clickhouse process")
    parser.add_argument("--log-file", help="Path to trampoline log file")
    args = parser.parse_args()

    setup_logging(args.log_file)

    logger.info("Trampoline started, args = %s", args)
    if args.version:
        print_version()
        exit(0)

    if not args.ytserver_clickhouse_bin:
        parser.error("Only positional argument 'ytserver_clickhouse_bin' should be present")
        exit(1)

    if args.prepare_geodata:
        extract_geodata()
    patch_ytserver_clickhouse_config(args.prepare_geodata)
    ytserver_clickhouse_process = run_ytserver_clickhouse(args.ytserver_clickhouse_bin, args.monitoring_port)
    sigint_handler = SigintHandler(ytserver_clickhouse_process, args.interrupt_child)
    log_tailer_process = None
    if args.log_tailer_bin:
        patch_log_tailer_config()
        log_tailer_process = run_log_tailer(args.log_tailer_bin,
            ytserver_clickhouse_process.pid, args.log_tailer_monitoring_port)
    logger.info("Waiting for ytserver-clickhouse to finish")
    exit_code = ytserver_clickhouse_process.wait()
    logger.info("ytserver-clickhouse exit code is %d", exit_code)
    logger.info("Waiting for log tailer to finish")
    if log_tailer_process is not None:
        log_tailer_exit_code = log_tailer_process.wait()
        logger.info("Log tailer exit code is %d", log_tailer_exit_code)
    if args.core_dump_destination:
        move_core_dumps(args.core_dump_destination)
    exit(exit_code)


if __name__ == "__main__":
    main()
