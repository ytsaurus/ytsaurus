from __future__ import print_function

import yt.logger as yt_logger

import os
import fcntl
import time
import shutil
import logging

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

def is_inside_arcadia(inside_arcadia):
    if inside_arcadia is None:
        inside_arcadia = int(yatest_common.get_param("inside_arcadia", True))
    return inside_arcadia

def is_inside_distbuild():
    if yatest_common is None:
        return False

    if yatest_common.get_param("teamcity"):
        return False

    if yatest_common.get_param("yt.localbuild"):
        return False

    return True

def get_root_paths(source_prefix="", inside_arcadia=None):
    if is_inside_arcadia(inside_arcadia):
        yt_root = source_prefix + "yt/19_4/"
        python_root = source_prefix + "yt/python/"
        global_root = source_prefix
    else:
        yt_root = ""
        python_root = "python/"
        global_root = ""
    return yt_root, python_root, global_root


SUDO_WRAPPER ="""#!/bin/sh

exec sudo -En {} {} {} {} "$@"
"""


def insert_sudo_wrapper(bin_dir):
    sudofixup = search_binary_path("yt-sudo-fixup")

    for binary in ["ytserver-exec", "ytserver-job-proxy", "ytserver-tools"]:
        bin_path = os.path.join(bin_dir, binary)
        orig_path = os.path.join(bin_dir, binary + ".orig")
        os.rename(bin_path, orig_path)

        with open(bin_path, "w") as trampoline:
            trampoline.write(SUDO_WRAPPER.format(sudofixup, os.getuid(), orig_path, binary))
            os.chmod(bin_path, 0o755)


def search_binary_path(binary_name):
    binary_root = yatest_common.binary_path('.')
    for dirpath, _, filenames in os.walk(binary_root):
        for f in filenames:
            if f == binary_name:
                return os.path.join(dirpath, binary_name)
    raise RuntimeError("binary {} is not found in {}".format(binary_name, binary_root))

    
def prepare_yt_binaries(destination, source_prefix="", arcadia_root=None, inside_arcadia=None, use_ytserver_all=False, use_from_package=False, copy_ytserver_all=False, need_suid=False):
    def get_binary_path(path):
        if arcadia_root is None:
            return search_binary_path(path)
        else:
            return os.path.join(arcadia_root, path)

    if use_ytserver_all:
        if use_from_package:
            ytserver_all = search_binary_path("ytserver-all")
        else:
            ytserver_all = None
            try:
                ytserver_all = get_binary_path("ytserver-all-stripped")
            except Exception:  # TestMisconfigurationException
                pass
            if ytserver_all is None or not os.path.exists(ytserver_all):
                ytserver_all = get_binary_path("ytserver-all")
        if copy_ytserver_all:
            shutil.copy(ytserver_all, os.path.join(destination, "ytserver-all"))
            ytserver_all = os.path.join(destination, "ytserver-all")

    else:
        assert not use_from_package

    programs = [("master", "master/bin"),
                ("clock", "clock_server/bin"),
                ("node", "node/bin"),
                ("job-proxy", "job_proxy/bin"),
                ("exec", "exec/bin"),
                ("proxy", "rpc_proxy/bin"),
                ("http-proxy", "http_proxy/bin"),
                ("tools", "tools/bin"),
                ("scheduler", "scheduler/bin"),
                ("controller-agent", "controller_agent/bin")]
    for binary, server_dir in programs:
        if use_ytserver_all:
            if copy_ytserver_all:
                os.link(ytserver_all, os.path.join(destination, "ytserver-" + binary))
            else:
                os.symlink(ytserver_all, os.path.join(destination, "ytserver-" + binary))
        else:
            binary_path = get_binary_path("ytserver-{0}".format(binary))
            os.symlink(binary_path, os.path.join(destination, "ytserver-" + binary))

    if need_suid:
        insert_sudo_wrapper(destination)

    watcher_path = get_binary_path("yt_env_watcher")
    shutil.copy(watcher_path, os.path.join(destination, "yt_env_watcher"))

    logrotate_path = get_binary_path("logrotate")
    shutil.copy(logrotate_path, os.path.join(destination, "logrotate"))

def prepare_yt_environment(destination, **kwargs):
    bin_dir = os.path.join(destination, "bin")
    lock_path = os.path.join(destination, "lock")
    prepared_path = os.path.join(destination, "prepared")

    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        while not os.path.exists(prepared_path):
            time.sleep(0.1)
        return bin_dir

    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)

        prepare_yt_binaries(bin_dir, **kwargs)

    with open(prepared_path, "w"):
        pass

    if yatest_common is not None:
        yt_logger.LOGGER = logging.getLogger()
        yt_logger.LOGGER.setLevel(logging.DEBUG)
        if yatest_common.context.test_stderr:
            yt_logger.LOGGER.addHandler(logging.StreamHandler())
        yt_logger.set_formatter(yt_logger.BASIC_FORMATTER)

    return bin_dir

def collect_cores(pids, working_directory, binaries, logger=None):
    cores_path = os.path.join(working_directory, "cores")
    if not os.path.isdir(cores_path):
        os.makedirs(cores_path)

    has_core_files = False
    for pid in pids:
        core_file = yatest_common.cores.recover_core_dump_file(
            # Temporarily collect all cores since problem with core file names.
            # yatest_common.binary_path("yp/server/master/bin/ypserver-master"),
            "*",
            # Process working directory.
            working_directory,
            pid)
        if core_file is not None:
            if logger is not None:
                logger.info("Core file found: " + core_file)
            try:
                shutil.move(core_file, cores_path)
            except IOError:
                # Ignore errors (it can happen for foreign cores).
                pass
            has_core_files = True

    if not has_core_files:
        if logger is not None:
            logger.debug("No core files found (working_directory: %s, pids: %s)",
                working_directory,
                str(pids))
    else:
        # Save binaries.
        for binary in binaries:
            shutil.copy(binary, cores_path)

def save_sandbox(sandbox_path, output_subpath):
    if yatest_common is None:
        return

    output_path = os.path.join(yatest_common.output_path(), output_subpath)
    if output_path == sandbox_path:
        return

    # Do not copy sandbox if it stored in output ram drive and consistent with output_subpath.
    if yatest_common.output_ram_drive_path() is not None and \
        sandbox_path.startswith(yatest_common.output_ram_drive_path()) and \
        sandbox_path.strip("/").endswith(output_subpath.strip("/")):
        return

    shutil.move(sandbox_path, output_path)

def get_gdb_path():
    if yatest_common is None:
        return "gdb"
    else:
        return yatest_common.gdb_path()
