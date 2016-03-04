import os
import sys
import logging
import shutil
import socket
import tarfile
import tempfile

import pytest
import yatest.common

YT_ARCH_NAME = "mapreduce/yt/python/yt.tgz" # comes by FROM_SANDBOX
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

    def _prepare_files(self):
        build_dir = yatest.common.runtime.build_path()
        self.yt_dir = tempfile.mkdtemp(prefix="yt_", dir=build_dir)

        self._log("Extracting YT")
        self._log(self.yt_dir)
        tgz = tarfile.open(os.path.join(build_dir, YT_ARCH_NAME))
        tgz.extractall(path=self.yt_dir)

        self.mapreduce_yt_path = os.path.join(self.yt_dir, "mapreduce-yt")
        self.yt_local_path = os.path.join(self.yt_dir, "yt_local")
        self.python_dir = os.path.join(self.yt_dir, "python")
        self.node_modules_dir = os.path.join(self.yt_dir, "node_modules")
        self.working_dir = os.path.join(self.yt_dir, "wd")

        os.mkdir(self.working_dir)

    def _prepare_env(self):
        self.env = {}
        self.env["PATH"] = self.yt_dir
        self.env["NODE_PATH"] = self.yt_dir + ":" + self.node_modules_dir
        self.env["PYTHONPATH"] = self.python_dir
        self.env["NODE_MODULES"] = self.node_modules_dir

    def _import_wrapper(self):
        sys.path.append(self.python_dir)
        import yt.wrapper
        self.yt_wrapper = yt.wrapper
        self.yt_wrapper.config.PREFIX = YT_PREFIX

    def _yt_local(self, *args):
        cmd = [sys.executable, self.yt_local_path] + list(args)
        self._log(" ".join([os.path.basename(cmd[0])] + cmd[1:]))
        res = yatest.common.process.execute(
            cmd,
            env=self.env,
            cwd=self.yt_dir
        )
        self._log(res.std_out)
        self._log(res.std_err)
        return res

    def get_yt_wrapper(self):
        return self.yt_wrapper

    def get_mapreduce_yt(self):
        return self.mapreduce_yt_path

    def start_local_yt(self):
        res = self._yt_local("start", "--path=" + self.working_dir)
        self.yt_id = res.std_out.strip()
        self.proxy_port = int(res.std_err.strip().splitlines()[-1].strip().split(":")[-1])
        self.yt_wrapper.config["proxy"]["url"] = "%s:%d" % (socket.gethostname(), self.proxy_port)
        self.yt_wrapper.config["proxy"]["enable_proxy_discovery"] = False

    def stop_local_yt(self):
        self._yt_local("stop", os.path.join(self.working_dir, self.yt_id))
        output_dir = yatest.common.output_path("yt")
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)
        self._log("YT logs saved in " + output_dir)
        for root, dirs, files in os.walk(self.working_dir):
            for file in files:
                if file.endswith(".log"):
                    shutil.copy2(os.path.join(root, file), os.path.join(output_dir, file))
        os.system("chmod -R 0775 " + output_dir)


@pytest.fixture(scope="module")
def yt_stuff(request):
    yt = YtStuff()
    yt.start_local_yt()
    request.addfinalizer(yt.stop_local_yt)
    return yt
