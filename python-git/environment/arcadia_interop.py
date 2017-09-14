import os
import sys
import subprocess

import yatest.common


def _extract_tar(tgz, where):
    subprocess.check_output(["tar", "-xf", tgz], cwd=where, stderr=subprocess.STDOUT)


def get_yt_versions():
    return os.listdir(yatest.common.build_path("yt/packages"))


def save_yt_binaries(dest, version):
    if version == "18_5":
        ytserver_path = yatest.common.binary_path("yt/packages/18_5/yt/18_5/yt/server/ytserver_program/ytserver")
        yt_server_custom_path = yatest.common.get_param("yt_ytserver_path")
        os.symlink(yt_server_custom_path or ytserver_path, os.path.join(dest, "ytserver"))
    else:
        for binary, server_dir in [("master", "cell_master_program"),
                                   ("scheduler", "cell_scheduler_program"),
                                   ("node", "cell_node_program"),
                                   ("job-proxy", "job_proxy_program"),
                                   ("exec", "exec_program")]:
            binary_path = yatest.common.binary_path("yt/packages/{0}/yt/{0}/yt/server/{1}/ytserver-{2}"
                                                    .format(version, server_dir, binary))
            os.symlink(binary_path, os.path.join(dest, "ytserver-" + binary))

        if version == "19_2":
            tools_binary_path = yatest.common.binary_path("yt/packages/19_2/yt/19_2/yt/server/tools_program/ytserver-tools")
            os.symlink(tools_binary_path, os.path.join(dest, "ytserver-tools"))

def save_yt_node(dest, version):
    yt_node_arcadia_path = yatest.common.binary_path("yt/packages/{0}/yt/{0}/yt/nodejs/targets/bin/ytnode".format(version))
    os.symlink(yt_node_arcadia_path, os.path.join(dest, "nodejs"))


def save_yt_node_modules(dest, version):
    node_modules_archive_path = yatest.common.binary_path("yt/packages/{0}/yt/{0}/yt/node_modules/resource.tar.gz".format(version))
    _extract_tar(node_modules_archive_path, dest)


def save_yt_nodejs_package(dest, version):
    yt_node_path = yatest.common.binary_path("yt/packages/{0}/yt/{0}/yt/nodejs/targets/package".format(version))
    os.symlink(yt_node_path, os.path.join(dest, "yt"))


def save_yt_thor(dest, version):
    yt_archive_path = yatest.common.binary_path("yt/packages/{0}/yt/packages/{0}/yt_thor.tar".format(version))
    _extract_tar(yt_archive_path, dest)

def prepare_path():
    version = "19_2"
    yt_build_dir = yatest.common.work_path("yt_build")
    os.mkdir(yt_build_dir)
    yt_build_bin_dir = os.path.join(yt_build_dir, "bin")
    os.mkdir(yt_build_bin_dir)
    save_yt_binaries(yt_build_bin_dir, version)

    os.symlink(yatest.common.binary_path(
        "yt/python/yt/environment/bin/yt_env_watcher_make/yt_env_watcher"),
        os.path.join(yt_build_bin_dir, "yt_env_watcher")
    )

    yt_build_node_dir = os.path.join(yt_build_dir, "node")
    os.mkdir(yt_build_node_dir)
    save_yt_node(yt_build_node_dir, version)

    save_yt_node_modules(yt_build_dir, version)

    yt_build_node_modules_dir = os.path.join(yt_build_dir, "node_modules")
    save_yt_nodejs_package(yt_build_node_modules_dir, version)

    path = ":".join([
        yt_build_dir,
        yt_build_bin_dir,
        yt_build_node_dir
    ])

    if "PATH" in os.environ:
        os.environ["PATH"] += ":" + path
    else:
        os.environ["PATH"] = path
    os.environ["NODE_PATH"] = ":".join([
        yt_build_node_dir,
        yt_build_node_modules_dir
    ])

