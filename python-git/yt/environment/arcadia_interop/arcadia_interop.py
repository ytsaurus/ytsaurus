import os
import fcntl
import time
import shutil

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None


def prepare_yt_binaries(destination, arcadia_root=None, yt_root="yt/19_4"):
    def get_binary_path(path):
        if arcadia_root is None:
            return yatest_common.binary_path(path)
        else:
            return os.path.join(arcadia_root, path)

    programs = [("master", "master/bin"),
                ("node", "node/bin"),
                ("job-proxy", "job_proxy/bin"),
                ("exec", "bin/exec"),
                ("proxy", "rpc_proxy/bin"),
                ("http-proxy", "http_proxy/bin"),
                ("tools", "bin/tools"),
                ("scheduler", "scheduler/bin"),
                ("controller-agent", "controller_agent/bin")]
    for binary, server_dir in programs:
        binary_path = get_binary_path("{0}/yt/server/{1}/ytserver-{2}"
                                      .format(yt_root, server_dir, binary))
        os.symlink(binary_path, os.path.join(destination, "ytserver-" + binary))

    watcher_path = get_binary_path(yt_root + "/python/yt/environment/bin/yt_env_watcher_make/yt_env_watcher")
    os.symlink(watcher_path, os.path.join(destination, "yt_env_watcher"))

    logrotate_path = get_binary_path(yt_root + "/infra/nanny/logrotate/logrotate")
    os.symlink(logrotate_path, os.path.join(destination, "logrotate"))

def prepare_yt_environment(destination, arcadia_root=None, yt_root="yt/19_4"):
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

        prepare_yt_binaries(bin_dir, arcadia_root=arcadia_root, yt_root=yt_root)

    with open(prepared_path, "w"):
        pass

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
    if output_path != sandbox_path:
        # TODO(ignat): Should we use shutil.move here?
        shutil.copytree(sandbox_path, output_path)
