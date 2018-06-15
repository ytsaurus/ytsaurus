import os
import fcntl
import time
import subprocess

import yatest.common

YT_ABI = "19_3"

def prepare_yt_binaries(destination):
    for binary, server_dir in [("master", "cell_master_program"),
                               ("scheduler", "programs/scheduler"),
                               ("node", "cell_node_program"),
                               ("proxy", "cell_proxy_program"),
                               ("job-proxy", "job_proxy_program"),
                               ("exec", "exec_program"),
                               ("tools", "tools_program"),
                               ("controller-agent", "programs/controller_agent")]:
        binary_path = yatest.common.binary_path("yt/{0}/yt/server/{1}/ytserver-{2}"
                                                .format(YT_ABI, server_dir, binary))
        os.symlink(binary_path, os.path.join(destination, "ytserver-" + binary))

    watcher_path = yatest.common.binary_path("yt/python/yt/environment/bin/yt_env_watcher_make/yt_env_watcher")
    os.symlink(watcher_path, os.path.join(destination, "yt_env_watcher"))

def prepare_nodejs(destination):
    path = yatest.common.binary_path("yt/{0}/yt/nodejs/targets/bin/ytnode".format(YT_ABI))
    os.symlink(path, os.path.join(destination, "nodejs"))

def prepare_nodejs_modules(destination):
    path = yatest.common.binary_path("yt/{0}/yt/node_modules/resource.tar.gz".format(YT_ABI))
    subprocess.check_output(["tar", "-xf", path], cwd=destination, stderr=subprocess.STDOUT)

def prepare_nodejs_yt_package(destination):
    path = yatest.common.binary_path("yt/{0}/yt/nodejs/targets/package".format(YT_ABI))
    os.symlink(path, os.path.join(destination, "yt"))

def prepare_yt_environment(destination):
    bin_dir = os.path.join(destination, "bin")
    node_modules_dir = os.path.join(destination, "node_modules")
    lock_path = os.path.join(destination, "lock")
    prepared_path = os.path.join(destination, "prepared")

    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        while not os.path.exists(prepared_path):
            time.sleep(0.1)
        return bin_dir, node_modules_dir

    if not os.path.exists(bin_dir):
        for dir_ in (bin_dir, node_modules_dir):
            os.makedirs(dir_)

        prepare_yt_binaries(bin_dir)
        prepare_nodejs(bin_dir)
        prepare_nodejs_modules(destination)
        prepare_nodejs_yt_package(node_modules_dir)

    with open(prepared_path, "w"):
        pass

    return bin_dir, node_modules_dir
