from __future__ import print_function

import yt.logger as yt_logger

import os
import fcntl
import time
import shutil
import logging
import subprocess

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

def sudo_rmtree(path):
    subprocess.check_call(["sudo", "rm", "-rf", path])

def is_inside_arcadia(inside_arcadia):
    if inside_arcadia is None:
        inside_arcadia = int(yatest_common.get_param("inside_arcadia", True))
    return inside_arcadia

def search_binary_path(binary_name, build_path_suffix=None, cwd_suffix=None):
    """
    Search for binary with given name somewhere in file system depending on keyword arguments.
    build_path_suffix and cwd_suffix should not be specified simultaneously. If none
    of them specified, search is performed in yatest_common.build_path().
    :param binary_name: name of the binary, e.g. ytserver-all or logrotate
    :param build_path_suffix: if present, subtree is yatest.common.build_path() + build_path_suffix
    :param cwd_suffix: if present, subtree is <cwd> + cwd_suffix
    :return:
    """
    assert build_path_suffix is None or cwd_suffix is None
    binary_root = yatest_common.build_path()
    if cwd_suffix is not None:
        binary_root = cwd_suffix
    if build_path_suffix is not None:
        binary_root = os.path.join(yatest_common.build_path(), build_path_suffix)
    binary_root = os.path.abspath(binary_root)

    for dirpath, _, filenames in os.walk(binary_root):
        for f in filenames:
            if f == binary_name:
                result = os.path.join(dirpath, binary_name)
                return result
    raise RuntimeError("binary {} is not found in {}".format(binary_name, binary_root))


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

def get_binary_path(path, arcadia_root, **kwargs):
    if arcadia_root is None:
        return search_binary_path(path, **kwargs)
    else:
        return os.path.join(arcadia_root, path)

PROGRAMS = [("master", "master/bin"),
            ("clock", "clock_server/bin"),
            ("timestamp-provider", "timestamp_provider/bin"),
            ("discovery", "discovery_server/bin"),
            ("node", "node/bin"),
            ("job-proxy", "job_proxy/bin"),
            ("exec", "exec/bin"),
            ("proxy", "rpc_proxy/bin"),
            ("http-proxy", "http_proxy/bin"),
            ("tools", "tools/bin"),
            ("scheduler", "scheduler/bin"),
            ("controller-agent", "controller_agent/bin"),
            ("master-cache", "master_cache/bin")]

def prepare_yt_binaries(destination,
                        source_prefix="", arcadia_root=None, inside_arcadia=None, use_ytserver_all=True,
                        use_from_package=False, package_suffix=None, copy_ytserver_all=False, ytserver_all_suffix=None,
                        need_suid=False, component_whitelist=None):
    if use_ytserver_all:
        if use_from_package:
            if package_suffix is not None:
                ytserver_all = search_binary_path("ytserver-all", cwd_suffix=package_suffix)
            else:
                ytserver_all = search_binary_path("ytserver-all", build_path_suffix="yt")
        else:
            ytserver_all = get_binary_path("ytserver-all", arcadia_root, build_path_suffix="yt")
        if copy_ytserver_all:
            ytserver_all_destination = os.path.join(destination, "ytserver-all")
            if ytserver_all_suffix is not None:
                ytserver_all_destination += "." + ytserver_all_suffix
            shutil.copy(ytserver_all, ytserver_all_destination)
            ytserver_all = ytserver_all_destination
    else:
        assert not use_from_package

    programs = PROGRAMS

    if component_whitelist is not None:
        programs = [(component, path) for component, path in programs if component in component_whitelist]

    for binary, server_dir in programs:
        if use_ytserver_all:
            dst_path = os.path.join(destination, "ytserver-" + binary)
            if copy_ytserver_all:
                os.link(ytserver_all, dst_path)
            else:
                os.symlink(ytserver_all, dst_path)
        else:
            binary_path = get_binary_path("ytserver-{0}".format(binary), arcadia_root)
            dst_path = os.path.join(destination, "ytserver-" + binary)
            os.symlink(binary_path, dst_path)

    if need_suid:
        insert_sudo_wrapper(destination)

def copy_misc_binaries(destination, arcadia_root=None):
    watcher_path = get_binary_path("yt_env_watcher", arcadia_root)
    shutil.copy(watcher_path, os.path.join(destination, "yt_env_watcher"))

    logrotate_path = get_binary_path("logrotate", arcadia_root)
    shutil.copy(logrotate_path, os.path.join(destination, "logrotate"))

def prepare_yt_environment(destination, artifact_components=None, **kwargs):
    artifact_components = artifact_components or {}
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

    # Compute which components should be taken from trunk and which from artifacts.

    all_components = [program for program, _ in PROGRAMS]

    trunk_components = set(program for program, _ in PROGRAMS)
    for artifact_name, components in artifact_components.items():
        for component in components:
            if component not in all_components:
                raise RuntimeError("Unknown artifact component {}; known components are {}".format(
                                   component, all_components))
            if component not in trunk_components:
                raise RuntimeError("Artifact component {} is specified multiple times".format(component))
            trunk_components.remove(component)

    if not os.path.exists(bin_dir):
        os.makedirs(bin_dir)

        if trunk_components:
            prepare_yt_binaries(bin_dir, component_whitelist=trunk_components, ytserver_all_suffix="trunk", **kwargs)

        for artifact_name, components in artifact_components.items():
            prepare_yt_binaries(bin_dir, component_whitelist=components, use_from_package=True,
                                package_suffix=os.path.join("artifact_bin", artifact_name, "result"),
                                ytserver_all_suffix=artifact_name, **kwargs)

    copy_misc_binaries(bin_dir, arcadia_root=kwargs.get("arcadia_root"))

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
            logger.debug(
                "No core files found (working_directory: %s, pids: %s)",
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

    if os.environ.get("YT_OUTPUT") is not None and sandbox_path.startswith(os.environ.get("YT_OUTPUT")):
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

def remove_runtime_data(working_directory):
    runtime_data_paths = []
    for root, dirs, files in os.walk(working_directory):
        if os.path.basename(root) == "runtime_data":
            runtime_data_paths.append(os.path.join(working_directory, root))

    yt_logger.info("Removing runtime data (paths: {})".format(", ".join(runtime_data_paths)))

    for path in runtime_data_paths:
        failed = False
        try:
            shutil.rmtree(path, ignore_errors=True)
        except IOError:
            yt_logger.exception("Failed to remove {} without sudo".format(path))
            failed = True

        if failed:
            try:
                sudo_rmtree(path)
            except subprocess.CalledProcessError:
                yt_logger.exception("Failed to remove {} with sudo".format(path))
