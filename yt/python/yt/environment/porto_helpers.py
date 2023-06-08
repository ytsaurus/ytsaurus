try:
    from yt.packages.six import iteritems
except ImportError:
    from six import iteritems

from yt.wrapper.common import generate_uuid

import logging

try:
    from shlex import quote
except ImportError:
    # Python2 doesn't have shelx.quote :(
    from pipes import quote

disable_porto = False
try:
    from porto import Connection, exceptions
except ImportError:
    disable_porto = True


logger = logging.getLogger("YtLocal")


def porto_available():
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
    if not porto_available():
        return

    conn = Connection()

    for volume in conn.ListVolumes():
        if volume.path.startswith(path):
            for c in volume.GetContainers():
                volume.Unlink(c)


class PortoSubprocess(object):
    @classmethod
    def Popen(cls, args, shell=None, close_fds=None, preexec_fn=None, cwd=None, stdout=None, stderr=None, env=None):
        command = " ".join(["'" + quote(a) + "'" for a in args])
        conn = Connection()

        for i in range(16):
            try:
                # Container name is limited to 200 symbols. When running inside YT job, 120 symbols are
                # taken by base infractructure. About 40 more symbols are used by YT node. We are left with
                # 40 symbols to start YT cluster with porto containers inside, so we can't afford to use full
                # GUID here.
                name = generate_uuid()[:4]
                container = conn.Create("self/" + str(name), weak=True)
                break
            except exceptions.ContainerAlreadyExists:
                if i == 15:
                    raise

        p = PortoSubprocess()
        p._container = container
        p._portoName = name
        p._connection = conn
        p._container.SetProperty("command", command)
        p._container.SetProperty("ulimit", "core: unlimited")
        p._container.SetProperty("isolate", "true")

        # TODO(prime@): enable controllers only when needed
        p._container.SetProperty("controllers[cpu]", "true")
        p._container.SetProperty("controllers[memory]", "true")

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

    def list_subcontainers(self):
        return [x for x in self._connection.List() if self._portoName in x]

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
            return key + "=" + value.replace(';', '\\;')
        if env is not None:
            environment = []
            for key, value in iteritems(env):
                environment.append(_format_key(key, value))
            self._container.SetProperty("env", ";".join(environment))
