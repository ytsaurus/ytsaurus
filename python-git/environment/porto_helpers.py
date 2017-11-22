disable_porto = False
try:
    from porto import Connection, exceptions
except ImportError:
    disable_porto = True

from pipes import quote

import sys
import os
import socket

from yt.wrapper.common import generate_uuid
import logging

logger = logging.getLogger("Yt.local")

def porto_avaliable():
    if disable_porto:
        return False

    conn = Connection()
    try:
        conn.connect()
        return True
    except socket.error as err:
        logger.exception("Failed to connect to porto, '%s'", err)
        return False

class PortoSubprocess(object):
    @classmethod
    def Popen(cls, args, shell=None, close_fds=None, preexec_fn=None, cwd=None, stdout=None, stderr=None):
        conn = Connection()
        name = generate_uuid()
        command = " ".join(["'" + quote(a) + "'" for a in args])
        p = PortoSubprocess()
        p._container = conn.Create(str(name))
        p._portoName = name
        p._connection = conn
        p._container.SetProperty("command", command)
        p._container.SetProperty("ulimit", "core: unlimited")
        p._container.SetProperty("porto_namespace", name + "/")
        p._container.SetProperty("isolate", "true")
        p._returncode = None
        p._copy_env()
        if stdout is not None:
            p._container.SetProperty("stdout_path", stdout.name)
        if stderr is not None:
            p._container.SetProperty("stderr_path", stderr.name)
        if cwd is not None:
            p._container.SetProperty("cwd", cwd)
        if preexec_fn is not None:
            logger.warning("preexec_fn is not implemented via porto")
        if not close_fds:
            logger.warning("fds inherit is not implemented via porto")

        p._container.Start()
        pid = p._container.GetProperty("root_pid")
        p._pid = int(pid)
        return p

    def poll(self):
        self._wait(0)
        return self._returncode

    def wait(self):
        self._wait(None)
        return self._returncode

    def destroy(self):
        self._container.Destroy()

    @property
    def returncode(self):
        return self._returncode

    @property
    def pid(self):
        return self._pid

    def __del__(self):
        try:
            self._container.Destroy()
        except exceptions.ContainerDoesNotExist:
            pass

    def _wait(self, timeout):
        if self._container.Wait(timeout):
            self._returncode = self._container.GetData("exit_status")

    def _copy_env(self):
        environment = ""
        for env, value in os.environ.iteritems():
            environment += env + "=" + value.replace(';','\\;') + ";"
        self._container.SetProperty("env", environment)
