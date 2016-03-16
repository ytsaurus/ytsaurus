import os
import logging
import shutil
import socket
import sys
import tempfile
import time

import pytest
import yatest.common

YT_ARCHIVE_NAME = "mapreduce/yt/python/yt.tgz" # comes by FROM_SANDBOX
YT_PREFIX = "//"

class YtStuff:
    def __init__(self):
        self._prepare_logger()
        self._prepare_files()
        self._prepare_env()
        self._import_wrapper()

    def _prepare_logger(self):
        self.logger = logging.getLogger()

    def _log(self, message):
        #print >>sys.stderr, message
        self.logger.debug(message)

    def _extract_tar(self, tgz, where):
        self._log("Extracting YT to %s" % self.yt_path)
        start_extracting = time.time()

        #import tarfile
        #tarfile.open(tgz).extractall(path=where)
        import subprocess
        subprocess.check_output(['tar', '-xf', tgz], cwd=where, stderr=subprocess.STDOUT)

        finish_extracting = time.time()
        self._log("Extracting time: %f" % (finish_extracting - start_extracting))

    def _prepare_files(self):
        build_path = yatest.common.runtime.build_path()
        work_path = yatest.common.runtime.work_path()

        self.tmpfs_path = yatest.common.get_param("ram_drive_path")
        if self.tmpfs_path:
            self.tmpfs_path = tempfile.mkdtemp(prefix="yt_", dir=self.tmpfs_path)

        # Folders
        self.yt_path = tempfile.mkdtemp(dir=work_path, prefix="yt_")
        self.yt_bins_path = os.path.join(self.yt_path, "bin")
        self.yt_python_path = os.path.join(self.yt_path, "python")
        self.yt_node_path = os.path.join(self.yt_path, "node")
        self.yt_node_bin_path = os.path.join(self.yt_node_path, "bin")
        self.yt_node_modules_path = os.path.join(self.yt_path, "node_modules")
        self.yt_thor_path = os.path.join(self.yt_path, "yt-thor")
        # Binaries
        self.mapreduce_yt_path = os.path.join(self.yt_bins_path, "mapreduce-yt")
        self.yt_local_path = os.path.join(self.yt_bins_path, "yt_local")

        yt_archive_path = os.path.join(build_path, YT_ARCHIVE_NAME)
        self._extract_tar(yt_archive_path, self.yt_path)

        self.yt_work_dir = os.path.join(self.yt_path, "wd")
        os.mkdir(self.yt_work_dir)

    def _prepare_env(self):
        self.env = {}
        self.env["PATH"] = ":".join([
                self.yt_bins_path,
                self.yt_node_path,
                self.yt_node_bin_path,
            ])
        self.env["NODE_MODULES"] = self.yt_node_modules_path
        self.env["NODE_PATH"] = ":".join([
                self.yt_node_path,
                self.yt_node_modules_path,
            ])
        self.env["PYTHONPATH"] = self.yt_python_path
        self.env["YT_LOCAL_THOR_PATH"] = self.yt_thor_path

    def _import_wrapper(self):
        sys.path.append(self.yt_python_path)
        import yt.wrapper
        self.yt_wrapper = yt.wrapper
        self.yt_wrapper.config.PREFIX = YT_PREFIX

    def _yt_local(self, *args):
        cmd = [sys.executable, self.yt_local_path] + list(args)
        self._log(" ".join([os.path.basename(cmd[0])] + cmd[1:]))
        res = yatest.common.process.execute(
            cmd,
            env=self.env,
            cwd=self.yt_work_dir
        )
        self._log(res.std_out)
        self._log(res.std_err)
        return res

    def get_yt_wrapper(self):
        return self.yt_wrapper

    def get_server(self):
        return "localhost:%d" % self.proxy_port

    def get_mapreduce_yt(self):
        return self.mapreduce_yt_path

    def get_env(self):
        return self.env

    def run_mapreduce_yt(self, *args):
        cmd = [sys.executable, self.mapreduce_yt_path] + list(args)
        return yatest.common.execute(
            cmd,
            env=self.env,
        )

    def start_local_yt(self):
        try:
            args = ["start", "--path=%s" % self.yt_work_dir]
            if self.tmpfs_path:
                args.append("--tmpfs-path=%s" % self.tmpfs_path)
            res = self._yt_local(*args)
        except Exception, e:
            self._log("Failed to start local YT:")
            self._log(str(e))
            self._save_server_logs()
            raise
        self.yt_id = res.std_out.strip()
        self.proxy_port = int(res.std_err.strip().splitlines()[-1].strip().split(":")[-1])
        self.yt_wrapper.config["proxy"]["url"] = "%s:%d" % (socket.gethostname(), self.proxy_port)
        self.yt_wrapper.config["proxy"]["enable_proxy_discovery"] = False

    def stop_local_yt(self):
        try:
            self._yt_local("stop", os.path.join(self.yt_work_dir, self.yt_id))
        except Exception, e:
            self._log("Errors while stopping local YT:")
            self._log(str(e))
            raise
        finally:
            self._save_server_logs()

    def _save_server_logs(self):
        output_dir = yatest.common.output_path("yt")
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)
        self._log("YT logs saved in " + output_dir)
        for root, dirs, files in os.walk(self.yt_work_dir):
            log_files = filter(lambda file: file.endswith(".log"), files)
            for file in log_files:
                for start in ["http_application", "http_proxy", "node", "master", "scheduler"]:
                    if file.startswith(start):
                        shutil.copy2(os.path.join(root, file), os.path.join(output_dir, file))
        os.system("chmod -R 0775 " + output_dir)


@pytest.fixture(scope="module")
def yt_stuff(request):
    yt = YtStuff()
    yt.start_local_yt()
    request.addfinalizer(yt.stop_local_yt)
    return yt
