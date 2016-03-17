from functools import wraps
import logging
import os
import shutil
import socket
import sys
import tempfile
import time

import pytest
import yatest.common

_YT_ARCHIVE_NAME = "mapreduce/yt/python/yt.tgz" # comes by FROM_SANDBOX
_YT_PREFIX = "//"

class YtStuff:
    def __init__(self):
        self._prepare_logger()
        self._prepare_files()
        self._prepare_env()
        self._import_wrapper()

    def _prepare_logger(self):
        self.logger = logging.getLogger()

    def _log(self, *args, **kwargs):
        #print >>sys.stderr, message
        self.logger.debug(*args, **kwargs)

    def _timing(method):
        @wraps(method)
        def wrap(self, *args, **kwargs):
            start_time = time.time()
            ret = method(self, *args, **kwargs)
            finish_time = time.time()
            self._log("%s time: %f", method.__name__, finish_time - start_time)
            return ret
        return wrap

    @_timing
    def _extract_tar(self, tgz, where):
        #import tarfile
        #tarfile.open(tgz).extractall(path=where)
        import subprocess
        subprocess.check_output(['tar', '-xf', tgz], cwd=where, stderr=subprocess.STDOUT)

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

        yt_archive_path = os.path.join(build_path, _YT_ARCHIVE_NAME)
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
        self.yt_wrapper.config.PREFIX = _YT_PREFIX

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

    def get_env(self):
        return self.env

    # Dear user, use mapreduce-yt correctly!
    # Don't forget to use sys.executable and to set environment!
    # Example: run_mapreduce_yt function.
    def get_mapreduce_yt(self):
        return self.mapreduce_yt_path

    def run_mapreduce_yt(self, *args):
        cmd = [sys.executable, self.mapreduce_yt_path] + list(args)
        return yatest.common.execute(
            cmd,
            env=self.env,
        )

    @_timing
    def start_local_yt(self):
        try:
            args = ["start", "--path=%s" % self.yt_work_dir]
            if self.tmpfs_path:
                args.append("--tmpfs-path=%s" % self.tmpfs_path)
            res = self._yt_local(*args)
        except Exception, e:
            self._log("Failed to start local YT:\n%s", str(e))
            self._save_yt_data(save_all=True)
            raise
        self.yt_id = res.std_out.strip()
        self.proxy_port = int(res.std_err.strip().splitlines()[-1].strip().split(":")[-1])
        self.yt_wrapper.config["proxy"]["url"] = "%s:%d" % (socket.gethostname(), self.proxy_port)
        self.yt_wrapper.config["proxy"]["enable_proxy_discovery"] = False

    @_timing
    def stop_local_yt(self):
        try:
            self._yt_local("stop", os.path.join(self.yt_work_dir, self.yt_id))
        except Exception, e:
            self._log("Errors while stopping local YT:\n%s", str(e))
            self._save_yt_data(save_all=True)
            raise
        self._save_yt_data(save_all=yatest.common.get_param("yt_save_all_data"))

    @_timing
    def _save_yt_data(self, save_all=None):
        output_dir = yatest.common.output_path("yt")
        self._log("YT data saved in %s", output_dir)

        def _ignore(path, names):
            IGNORE_DIRS_ALWAYS = ["ui"]
            IGNORE_DIRS = ["chunk_store", "chunk_cache", "changelogs", "snapshots"]
            ignored = set()
            for name in names:
                full_path = os.path.join(path, name)
                if os.path.islink(full_path):
                    ignored.add(name)
                elif os.path.isdir(full_path):
                    should_ignore = False
                    should_ignore |= name in IGNORE_DIRS_ALWAYS
                    should_ignore |= not save_all and name in IGNORE_DIRS
                    if should_ignore:
                        ignored.add(name)
            return ignored

        shutil.copytree(src=self.yt_work_dir, dst=output_dir, ignore=_ignore)
        os.system("chmod -R 0775 " + output_dir)


@pytest.fixture(scope="module")
def yt_stuff(request):
    yt = YtStuff()
    yt.start_local_yt()
    request.addfinalizer(yt.stop_local_yt)
    return yt
