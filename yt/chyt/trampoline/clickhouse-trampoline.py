#!/usr/bin/python3

import argparse
import ctypes
import logging
import re
import tarfile
import os
import os.path
import glob
import subprocess
import signal
import time
import requests
import yt.yson

logger = logging.getLogger("clickhouse-trampoline")


class SigintHandler(object):
    def __init__(self, ytserver_clickhouse_process, interrupt_child):
        self.original_sigint_handler = signal.getsignal(signal.SIGINT)
        self.sigint_index = 0
        self.ytserver_clickhouse_process = ytserver_clickhouse_process
        self.interrupt_child = interrupt_child

    def install(self):
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


def extract_geodata():
    logger.info("Extracting geodata")
    assert os.path.exists("./geodata.tgz")
    tar = tarfile.open("./geodata.tgz", "r:gz")
    os.mkdir("./geodata")
    tar.extractall(path="./geodata/")
    tar.close()
    logger.info("Geodata extracted")


ODBC_INI_NAMES = ("odbcinst.ini", "odbc.ini")
ODBC_CONFIG_DIR = "odbc"
JDBC_BRIDGE_HOST = "127.0.0.1"
JDBC_BRIDGE_PORT_ENV = "YT_PORT_5"
PR_SET_PDEATHSIG = 1

UNEXPANDED_SECRET_RE = re.compile(r"\$\{?(YT_SECURE_VAULT_\w*)\}?")


def prepare_odbc():
    cwd = os.getcwd()

    if not all(os.path.exists(os.path.join(cwd, name)) for name in ODBC_INI_NAMES):
        logger.warning("odbcinst.ini or odbc.ini are not present in the job sandbox, skipping ODBC preparation")
        return

    logger.info("Preparing ODBC configuration")

    # The generated odbcinst.ini locates driver libraries via $PWD, which is not guaranteed
    # to be present in the job environment.
    os.environ["PWD"] = cwd

    odbc_dir = os.path.join(cwd, ODBC_CONFIG_DIR)
    os.makedirs(odbc_dir, exist_ok=True)

    for name in ODBC_INI_NAMES:
        with open(os.path.join(cwd, name), "r") as f:
            content = f.read()
        content = os.path.expandvars(content)
        unexpanded = sorted(set(UNEXPANDED_SECRET_RE.findall(content)))
        if unexpanded:
            logger.warning("Secrets referenced from %s are missing in the job environment: %s", name, unexpanded)
        # NB: the sandbox artifacts may be read-only links into the chunk cache, so the expanded
        # copies are written next to them rather than in place.
        with open(os.path.join(odbc_dir, name), "w") as f:
            f.write(content)

    os.environ["ODBCSYSINI"] = odbc_dir
    os.environ["ODBCINI"] = os.path.join(odbc_dir, "odbc.ini")
    os.environ["LD_LIBRARY_PATH"] = os.pathsep.join(
        filter(None, [cwd, os.environ.get("LD_LIBRARY_PATH")]))
    logger.info("Set ODBCSYSINI=%s, ODBCINI=%s, LD_LIBRARY_PATH=%s",
                os.environ["ODBCSYSINI"], os.environ["ODBCINI"], os.environ["LD_LIBRARY_PATH"])

    logger.info("ODBC configuration prepared")


def substitute_env(content):
    content = content.replace("$YT_JOB_INDEX", os.environ["YT_JOB_INDEX"])
    content = content.replace("$YT_JOB_COOKIE", os.environ["YT_JOB_COOKIE"])
    return content


def inject_tvm_secret(config):
    if "native_authentication_manager" not in config:
        return
    secret_path = os.path.join(os.getcwd(), "tvm_secret")
    if not os.path.exists(secret_path):
        secret = os.getenv("YT_SECURE_VAULT_TVM_SECRET")
        if secret is None:
            logger.warning("No secret for native TVM authentication found")
            return
        with open(secret_path, "w") as f:
            f.write(secret.strip())
    config["native_authentication_manager"]["tvm_service"]["client_self_secret_path"] = secret_path


def disable_fqdn_resolution(config):
    # In MTN there may be no reasonable fqdn;
    # hostname returns something human-readable, but barely resolvable.
    config["address_resolver"] = config.get("address_resolver", dict())
    config["address_resolver"]["resolve_hostname_into_fqdn"] = False


def patch_ytserver_clickhouse_config(prepare_geodata, inside_mtn, jdbc_bridge_port=None):
    logger.info("Patching ytserver-clickhouse config")
    assert os.path.exists("./config.yson")
    with open("./config.yson", "r") as f:
        content = f.read()
    content = substitute_env(content)
    if not prepare_geodata:
        content = "\n".join(filter(lambda line: "./geodata" not in line, content.split("\n")))
    config = yt.yson.loads(str.encode(content))
    if jdbc_bridge_port is not None:
        bridge_config = config["clickhouse"].setdefault("jdbc_bridge", {})
        bridge_config.update(host=JDBC_BRIDGE_HOST, port=jdbc_bridge_port)
    if inside_mtn:
        logger.info("Disabling FQDN resolution in ytserver-clickhouse config")
        disable_fqdn_resolution(config)
    inject_tvm_secret(config)
    content = yt.yson.dumps(config, yson_format="pretty")
    with open("./config_patched.yson", "w") as f:
        f.write(content.decode("utf-8"))
    logger.info("Config patched")


def patch_log_tailer_config(inside_mtn):
    logger.info("Patching log tailer config")
    assert os.path.exists("./log_tailer_config.yson")
    with open("./log_tailer_config.yson", "r") as f:
        content = f.read()
    content = substitute_env(content)
    config = yt.yson.loads(str.encode(content))
    if inside_mtn:
        logger.info("Disabling fqdn resolution in ytserver-log-tailer config")
        disable_fqdn_resolution(config)
    inject_tvm_secret(config)
    content = yt.yson.dumps(config, yson_format="pretty")
    with open("./log_tailer_config_patched.yson", "w") as f:
        f.write(content.decode("utf-8"))
    logger.info("Config patched")


def start_process(args, shell=False, parent_death_signal=None):
    logger.info("Going to invoke following command: %s", args)
    # NB: without preexec_fn=os.setpgrp any signal coming to the parent will always be immediately propagated to
    # children.
    # See https://stackoverflow.com/questions/3791398/how-to-stop-python-from-propagating-signals-to-subprocesses for
    # more details.
    parent_pid = os.getpid()

    def configure_child():
        os.setpgrp()
        if parent_death_signal is not None:
            libc = ctypes.CDLL(None, use_errno=True)
            if libc.prctl(PR_SET_PDEATHSIG, parent_death_signal) != 0:
                errno = ctypes.get_errno()
                raise OSError(errno, os.strerror(errno))
            if os.getppid() != parent_pid:
                os.kill(os.getpid(), parent_death_signal)

    kwargs = {"preexec_fn": configure_child}
    if shell:
        kwargs["shell"] = True
        kwargs["executable"] = "/bin/bash"
    process = subprocess.Popen(args, **kwargs)
    logger.info("Process started, pid = %d", process.pid)
    return process


def stop_process(name, process):
    if process.poll() is not None:
        return

    logger.info("Stopping %s", name)
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        logger.warning("%s did not stop in 10 seconds, killing it", name)
        process.kill()
        process.wait()


def run_jdbc_bridge(jdbc_trampoline_bin, port):
    logger.info("Starting JDBC bridge")

    process = start_process([jdbc_trampoline_bin, "--port", str(port)], parent_death_signal=signal.SIGTERM)
    http_address = "http://{}:{}/ping".format(JDBC_BRIDGE_HOST, port)

    deadline = time.time() + 60
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                "JDBC bridge exited before becoming ready with code {}".format(process.returncode))
        try:
            if requests.get(http_address, timeout=1).content.strip() == b"Ok.":
                logger.info("JDBC bridge is ready")
                return process
        except requests.RequestException:
            pass
        time.sleep(1)

    stop_process("JDBC bridge", process)
    raise RuntimeError("JDBC bridge did not become ready in 60 seconds")


def wait_for_ytserver_clickhouse(ytserver_clickhouse_process, jdbc_bridge_process):
    if jdbc_bridge_process is None:
        return ytserver_clickhouse_process.wait()

    while ytserver_clickhouse_process.poll() is None and jdbc_bridge_process.poll() is None:
        time.sleep(1)

    if jdbc_bridge_process.poll() is not None and ytserver_clickhouse_process.poll() is None:
        logger.error("JDBC bridge exited unexpectedly with code %d", jdbc_bridge_process.returncode)
        ytserver_clickhouse_process.terminate()

    return ytserver_clickhouse_process.wait()


def run_ytserver_clickhouse(ytserver_clickhouse_bin, monitoring_port, stderr_file):
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
    shell = False
    if stderr_file is not None:
        args = "exec " + " ".join(args)
        stderr_file = substitute_env(stderr_file)
        args += " 2> >(tee {} >&2)".format(stderr_file)
        shell = True
    return start_process(args, shell)


def run_log_tailer(log_tailer_bin, ytserver_clickhouse_pid, monitoring_port):
    logger.info("Running log tailer over pid %d", ytserver_clickhouse_pid)
    args = [log_tailer_bin, str(ytserver_clickhouse_pid), "--config", "./log_tailer_config_patched.yson"]
    args += ["--monitoring-port", monitoring_port or os.environ["YT_PORT_4"]]
    return start_process(args)


def list_core_dumps():
    return glob.glob("./core*")


def move_core_dumps(destination):
    logger.info("Hardlinking core dumps to %s", destination)
    for file in list_core_dumps():
        logger.debug("Hardlinking %s to point to %s", destination, file)
        logger.debug("Core %s size is %s", file, os.stat(file).st_size)
        destination_file = os.path.join(destination, file)
        os.link(file, destination_file)
        logger.debug("%s moved", file)
    logger.info("Core dumps hardlinked")


def print_version():
    try:
        import library.python.svn_version
        print("0.0.{}~{}".format(library.python.svn_version.svn_revision(), library.python.svn_version.hash()))
    except (ImportError, AttributeError):
        print("0.0.{}~{}".format(0, 0))


def setup_logging(log_file):
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s %(levelname)s\t%(message)s")
    stderr_handler = logging.StreamHandler()
    stderr_handler.setLevel(logging.DEBUG)
    stderr_handler.setFormatter(formatter)
    logger.addHandler(stderr_handler)

    if log_file:
        log_file = substitute_env(log_file)

    file_handler = logging.FileHandler(log_file or "trampoline.debug.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def is_inside_mtn():
    return any(key in os.environ for key in ("YT_IP_ADDRESS_FB", "YT_IP_ADDRESS_BB",
                                             "YT_IP_ADDRESS_DEFAULT", "YT_IP_ADDRESS_FASTBONE",
                                             "YT_IP_ADDRESS_BACKBONE"))


def wait_for_readiness(timeout, ytserver_clickhouse_process):
    http_address = "http://localhost:{}".format(os.environ["YT_PORT_3"])
    logger.info("Checking HTTP server readiness (timeout = %s sec, http_address = %s)", timeout, http_address)
    start_time = time.time()
    # Just in case some request hangs during HTTP server initialization.
    REQUEST_TIMEOUT = 3
    BACKOFF = 3
    while time.time() - start_time < timeout:
        # Check if clickhouse process is not dead yet.
        if ytserver_clickhouse_process.poll() is not None:
            logger.info("ytserver-clickhouse is already dead, stopping checking readiness")
            return
        try:
            if requests.get(http_address, timeout=REQUEST_TIMEOUT).content == b'Ok.\n':
                logger.info("HTTP server ready")
                return
        except Exception:
            logger.exception("Exception while making HTTP request")
        time.sleep(BACKOFF)
    logger.error("HTTP server is not ready for %s seconds, sending SIGABRT to ytserver-clickhouse", timeout)
    ytserver_clickhouse_process.send_signal(signal.SIGABRT)
    exit_code = ytserver_clickhouse_process.wait()
    core_dumps = list_core_dumps()
    logger.info("Core dumps: %s", core_dumps)
    for core_dump in core_dumps:
        gdb_args = ["gdb", "--batch", "-c", core_dump, "-ex", "bt"]
        logger.info("Invoking GDB for %s", core_dump)
        gdb_process = start_process(gdb_args)
        exit_code = gdb_process.wait()
        logger.info("GDB exit code is %s", exit_code)


def main():
    parser = argparse.ArgumentParser(description="Process that setups environment for running ytserver-clickhouse")
    parser.add_argument("--version", action="store_true", help="Print commit this binary is built from")
    parser.add_argument("ytserver_clickhouse_bin", nargs="?", help="ytserver-clickhouse binary path")
    parser.add_argument("--prepare-geodata", action="store_true", help="Extract archive with geodata")
    parser.add_argument("--prepare-odbc", action="store_true", help="Prepare ODBC env variables")
    parser.add_argument("--prepare-jdbc", action="store_true", help="Start JDBC bridge")
    parser.add_argument("--jdbc-trampoline-bin", help="JDBC trampoline executable path")
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
    parser.add_argument("--stderr-file", help="Redirect stderr to this file (substituting $YT_JOB_INDEX from env, "
                                              "if needed)")
    parser.add_argument("--readiness-timeout", type=int,
                        help="Timeout in seconds; if CH process is not replying 'Ok.' on HTTP port after this timeout, "
                             "process will be killed with SIGABRT and core dump will (hopefully) be produced")
    args = parser.parse_args()

    if args.version:
        print_version()
        exit(0)

    setup_logging(args.log_file)

    logger.info("Trampoline started, args = %s", args)

    if not args.ytserver_clickhouse_bin:
        parser.error("Only positional argument 'ytserver_clickhouse_bin' should be present")
        exit(1)

    if args.prepare_geodata:
        extract_geodata()

    if args.prepare_odbc:
        prepare_odbc()

    jdbc_bridge_process = None
    jdbc_bridge_port = None
    if args.prepare_jdbc:
        if not args.jdbc_trampoline_bin:
            parser.error("--jdbc-trampoline-bin is required with --prepare-jdbc")
        jdbc_bridge_port = int(os.environ[JDBC_BRIDGE_PORT_ENV])
        jdbc_bridge_process = run_jdbc_bridge(args.jdbc_trampoline_bin, jdbc_bridge_port)

    inside_mtn = is_inside_mtn()
    if inside_mtn:
        logger.info("Apparently we are inside MTN")

    patch_ytserver_clickhouse_config(args.prepare_geodata, inside_mtn, jdbc_bridge_port)

    stderr_file = args.stderr_file

    # TODO(max42): there are some issues around signaling; wait until log tailer is deprecated.
    if args.log_tailer_bin:
        stderr_file = None

    ytserver_clickhouse_process = run_ytserver_clickhouse(args.ytserver_clickhouse_bin, args.monitoring_port,
                                                          stderr_file)
    sigint_handler = SigintHandler(ytserver_clickhouse_process, args.interrupt_child)
    sigint_handler.install()

    log_tailer_process = None
    if args.log_tailer_bin:
        patch_log_tailer_config(inside_mtn)
        logger.info("Running ytserver-log-tailer")
        log_tailer_process = run_log_tailer(
            args.log_tailer_bin, ytserver_clickhouse_process.pid, args.log_tailer_monitoring_port)

    if args.readiness_timeout:
        wait_for_readiness(args.readiness_timeout, ytserver_clickhouse_process)

    logger.info("Waiting for ytserver-clickhouse to finish")
    exit_code = wait_for_ytserver_clickhouse(ytserver_clickhouse_process, jdbc_bridge_process)
    logger.info("ytserver-clickhouse exit code is %d", exit_code)

    if jdbc_bridge_process is not None:
        stop_process("JDBC bridge", jdbc_bridge_process)

    if exit_code < 0:
        exit_code = 128 + abs(exit_code)

    logger.info("Waiting for log tailer to finish")
    if log_tailer_process is not None:
        log_tailer_exit_code = log_tailer_process.wait()
        logger.info("Log tailer exit code is %d", log_tailer_exit_code)
    if args.core_dump_destination:
        move_core_dumps(args.core_dump_destination)
    exit(exit_code)


if __name__ == "__main__":
    main()
