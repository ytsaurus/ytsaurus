disable_porto = False
try:
    from porto import Connection, exceptions
except ImportError:
    disable_porto = True

from yt.packages.six import iteritems

from yt.wrapper.common import generate_uuid

import os
import socket
import logging
from pipes import quote

logger = logging.getLogger("Yt.local")

def porto_avaliable():
    global disable_porto
    if disable_porto:
        return False

    conn = Connection()
    try:
        conn.connect()
        return True
    except Exception as err:
        disable_porto = True
        logger.exception("Failed to connect to porto, '%s'", err)
        return False

def remove_all_volumes(path):
    if not porto_avaliable():
        return

    conn = Connection()

    for volume in conn.ListVolumes():
        if volume.path.startswith(path):
            for c in volume.GetContainers():
                volume.Unlink(c)


class PortoSubprocess(object):
    @classmethod
    def Popen(cls, args, shell=None, close_fds=None, preexec_fn=None, cwd=None, stdout=None, stderr=None, env=None):
        conn = Connection()
        name = generate_uuid()
        command = " ".join(["'" + quote(a) + "'" for a in args])
        p = PortoSubprocess()
        p._container = conn.Create(str(name), weak=True)
        p._portoName = name
        p._connection = conn
        p._container.SetProperty("command", command)
        p._container.SetProperty("ulimit", "core: unlimited")
        p._container.SetProperty("porto_namespace", name + "/")
        p._container.SetProperty("isolate", "true")
        p._returncode = None
        p._set_env(env)
        if stdout is not None:
            p._container.SetProperty("stdout_path", stdout.name)
        if stderr is not None:
            p._container.SetProperty("stderr_path", stderr.name)
        if cwd is not None:
            p._container.SetProperty("cwd", cwd)
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

    def set_cpu_limit(self, cpu_limit):
        self._container.SetProperty("cpu_limit", "{}c".format(cpu_limit))

    def set_memory_limit(self, memory_limit):
        self._container.SetProperty("memory_limit", "{}".format(memory_limit))

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

    def _set_env(self, env):
        def _format_key(key, value):
            return key + "=" + value.replace(';','\\;')
        if env is not None:
            environment = []
            for key, value in iteritems(env):
                environment.append(_format_key(key, value))
            self._container.SetProperty("env", ";".join(environment))
