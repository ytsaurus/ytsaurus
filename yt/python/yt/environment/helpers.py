from __future__ import print_function

from yt.wrapper.common import generate_uuid

from yt.common import to_native_str, YtError, YtResponseError, which  # noqa

import signal
import sys

# COMPAT for tests.
try:
    from yt.test_helpers import (  # noqa
        are_almost_equal, wait, unorderable_list_difference,
        assert_items_equal, are_items_equal, WaitFailed, Counter)
    assert_almost_equal = are_almost_equal
except ImportError:
    pass

try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

import yt.yson as yson

try:
    from yt.packages.six import iteritems, PY3, text_type, Iterator
    from yt.packages.six.moves import xrange, map as imap
except ImportError:
    from six import iteritems, PY3, text_type, Iterator
    from six.moves import xrange, map as imap

import codecs
import collections
import errno
import fcntl
import logging
import os
import random
import socket
import subprocess
import time
import contextlib

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

if yatest_common is not None:
    from yatest.common import network as yatest_common_network
else:
    yatest_common_network = None

logger = logging.getLogger("YtLocal")

if PY3:
    def cmp(a, b):
        return (a > b) - (a < b)


def _dump_netstat(dump_file_path):
    logger.info("Dumping netstat to the file '{}'".format(dump_file_path))
    with open(dump_file_path, "wb") as dump_file:
        subprocess.check_call(["netstat", "-v", "-p", "-a", "-ee"], stdout=dump_file)


class OpenPortIterator(Iterator):
    def __init__(self, port_locks_path=None, local_port_range=None):
        if yatest_common_network is None:
            self._impl = OpenPortIteratorNonArcadia(port_locks_path, local_port_range)
        else:
            self._impl = OpenPortIteratorArcadia()

    def __iter__(self):
        return self

    def __next__(self):
        return self._impl.__next__()

    def release(self):
        return self._impl.release()


class OpenPortIteratorArcadia(Iterator):
    def __init__(self):
        if yatest_common_network is None:
            raise RuntimeError("Cannot use OpenPortIteratorArcadia outside arcadia")
        self.port_manager = yatest_common_network.PortManager()

    def release(self):
        self.port_manager.release()

    def __next__(self):
        return self.port_manager.get_port()


class OpenPortIteratorNonArcadia(Iterator):
    GEN_PORT_ATTEMPTS = 10
    START_PORT = 10000

    def __init__(self, port_locks_path=None, local_port_range=None):
        self.busy_ports = set()

        self.port_locks_path = port_locks_path
        self.lock_fds = set()

        self._random_generator = None

        self.local_port_range = local_port_range
        if self.local_port_range is None and os.path.exists("/proc/sys/net/ipv4/ip_local_port_range"):
            with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
                start, end = list(imap(int, f.read().split()))
                self.local_port_range = start, min(end, start + 10000)

    def release(self):
        for lock_path, lock_fd in self.lock_fds:
            try:
                os.close(lock_fd)
                try:
                    os.remove(lock_path)
                except IOError:
                    pass
            except OSError as err:
                logger.warning("Failed to close file descriptor %d: %s",
                               lock_fd, os.strerror(err.errno))

    def __iter__(self):
        return self

    def _next_impl(self, verbose, error_counter=None):
        if error_counter is None:
            error_counter = collections.Counter()

        port = None
        if self.local_port_range is not None and self.local_port_range[0] - self.START_PORT > 1000:
            port_range = (self.START_PORT, self.local_port_range[0] - 1)
            if verbose:
                logger.info("Generating port by randomly selecting from the range: {}".format(port_range))
            if self._random_generator is None:
                self._random_generator = random.Random(random.SystemRandom().random())
            port_value = self._random_generator.randint(*port_range)
            if _is_port_free(port_value, verbose):
                port = port_value
            else:
                error_counter["is_port_free"] += 1
        else:
            if verbose:
                logger.info("Generating port by binding to zero port")
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(("", 0))
                sock.listen(1)
                port_value = sock.getsockname()[1]
            finally:
                sock.close()
            if _is_port_free(port_value, verbose):
                port = port_value
            else:
                error_counter["is_port_free"] += 1

        if port is None:
            return None

        if port in self.busy_ports:
            error_counter["busy_port"] += 1
            return None

        if self.port_locks_path is not None:
            lock_path = os.path.join(self.port_locks_path, str(port))
            lock_fd = None
            try:
                lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
                fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                error_counter["lock_file"] += 1
                if verbose:
                    logger.exception(
                        "Exception occurred while trying to lock port path '{}'".format(lock_path)
                    )
                if lock_fd is not None and lock_fd != -1:
                    os.close(lock_fd)
                    try:
                        os.remove(lock_path)
                    except IOError:
                        pass
                self.busy_ports.add(port)
                return None

            self.lock_fds.add((lock_path, lock_fd))

        self.busy_ports.add(port)

        return port

    def __next__(self):
        error_counter = collections.Counter()
        for _ in xrange(self.GEN_PORT_ATTEMPTS):
            port = self._next_impl(verbose=False, error_counter=error_counter)
            if port is not None:
                return port
        else:
            logger.warning("Failed to generate open port after %d attempts", self.GEN_PORT_ATTEMPTS)
            logger.warning("Number of port files: %d", len(os.listdir(self.port_locks_path)))
            logger.warning("Error counts: %s", dict(error_counter))

            logger.warning("Trying to infer reasons via verbose invocation:")
            self._next_impl(verbose=True)

            if self.port_locks_path is not None:
                try:
                    _dump_netstat(os.path.join(self.port_locks_path, "netstat-" + generate_uuid()))
                except:  # noqa
                    logger.exception("Exception occurred while dumping netstat")

            raise RuntimeError("Failed to generate open port after {0} attempts"
                               .format(self.GEN_PORT_ATTEMPTS))


def _is_port_free_for_inet(port, inet, verbose):
    sock = None
    try:
        sock = socket.socket(inet, socket.SOCK_STREAM)
        sock.bind(("", port))
        sock.listen(1)
        return True
    except:  # noqa
        if verbose:
            logger.exception(
                "Exception occurred while trying to check port freeness "
                "for port {} and inet {}".format(
                    port,
                    inet,
                )
            )
        return False
    finally:
        if sock is not None:
            sock.close()


def _is_port_free(port, verbose):
    return _is_port_free_for_inet(port, socket.AF_INET, verbose) and \
        _is_port_free_for_inet(port, socket.AF_INET6, verbose)


def is_port_opened(port, verbose=False):
    return not _is_port_free(port, verbose)


def versions_cmp(version1, version2):
    def normalize(v):
        return list(imap(int, v.split(".")))
    return cmp(normalize(version1), normalize(version2))


def _fix_yson_booleans(obj):
    if isinstance(obj, dict):
        for key, value in list(iteritems(obj)):
            _fix_yson_booleans(value)
            if isinstance(value, yson.YsonBoolean):
                obj[key] = True if value else False
    elif isinstance(obj, list):
        for value in obj:
            _fix_yson_booleans(value)
    return obj


def write_config(config, filename, format="yson"):
    with open(filename, "wb") as f:
        if format == "json":
            writer = lambda stream: stream  # noqa
            if PY3:
                writer = codecs.getwriter("utf-8")
            json.dump(_fix_yson_booleans(config), writer(f), indent=4)
        elif format == "yson":
            yson.dump(config, f, yson_format="pretty")
        else:
            if isinstance(config, text_type):
                config = config.encode("utf-8")
            f.write(config)
        f.write(b"\n")


def read_config(filename, format="yson"):
    with open(filename, "rb") as f:
        if format == "yson":
            return yson.load(f)
        elif format == "json":
            reader = lambda stream: stream  # noqa
            if PY3:
                reader = codecs.getreader("utf-8")
            return json.load(reader(f))
        else:
            return to_native_str(f.read())


def is_dead(pid, verbose=False):
    try:
        waitpid_ret = os.waitpid(pid, os.WNOHANG)
    except OSError as e:
        waitpid_ret = e

    if verbose:
        logger.info("XXX os.waitpid({}, WNOHANG) returned {!r}".format(pid, waitpid_ret))

    try:
        with open("/proc/{0}/status".format(pid), "r"):
            return False
    except IOError:
        pass

    return True


def is_zombie(pid):
    try:
        with open("/proc/{0}/status".format(pid), "r") as f:
            for line in f:
                if line.startswith("State:"):
                    return line.split()[1] == "Z"
    except IOError:
        pass

    return False


def is_file_locked(lock_file_path):
    if not os.path.exists(lock_file_path):
        return False

    lock_file_descriptor = open(lock_file_path, "w+")
    try:
        fcntl.lockf(lock_file_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.lockf(lock_file_descriptor, fcntl.LOCK_UN)
        return False
    except IOError as error:
        if error.errno == errno.EAGAIN or error.errno == errno.EACCES:
            return True
        raise
    finally:
        lock_file_descriptor.close()


def wait_for_removing_file_lock(lock_file_path, max_wait_time=10, sleep_quantum=0.1):
    current_wait_time = 0
    while current_wait_time < max_wait_time:
        if not is_file_locked(lock_file_path):
            return

        time.sleep(sleep_quantum)
        current_wait_time += sleep_quantum

    raise YtError("File lock is not removed after {0} seconds".format(max_wait_time))


def canonize_uuid(uuid):
    def canonize_part(part):
        if part != "0":
            return part.lstrip("0")
        return part
    return "-".join(map(canonize_part, uuid.split("-")))


def get_value_from_config(config, key, name):
    d = config
    parts = key.split("/")
    for k in parts:
        d = d.get(k)
        if d is None:
            raise YtError('Failed to get required key "{0}" from {1} config'.format(key, name))
    return d


def emergency_exit_within_tests(test_environment, process, call_arguments):
    if int(process.returncode) < 0:
        what = "terminated by signal {}".format(-process.returncode)
    else:
        what = "exited with code {}".format(process.returncode)

    print('Process run by command "{0}" {1}'.format(" ".join(call_arguments), what), file=sys.stderr)
    test_environment.stop()

    print("Killing pytest process", file=sys.stderr)
    # Avoid dumping useless stacktrace to stderr.
    os.kill(os.getpid(), signal.SIGKILL)


SCHEDULERS_SERVICE = "schedulers"
CONTROLLER_AGENTS_SERVICE = "controller_agents"
NODES_SERVICE = "nodes"
CHAOS_NODES_SERVICE = "chaos_nodes"
MASTERS_SERVICE = "masters"
MASTER_CACHES_SERVICE = "master_caches"
QUEUE_AGENTS_SERVICE = "queue_agents"
RPC_PROXIES_SERVICE = "rpc_proxies"
HTTP_PROXIES_SERVICE = "http_proxies"
KAFKA_PROXIES_SERVICE = "kafka_proxies"


class Restarter(object):
    def __init__(self, yt_instance, components, sync=True, start_bin_path=None, *args, **kwargs):
        self.yt_instance = yt_instance
        self.components = components
        if type(self.components) is str:
            self.components = [self.components]
        self.sync = sync
        self.start_custom_paths = [start_bin_path] if start_bin_path is not None else None
        self.kill_args = args
        self.kill_kwargs = kwargs

        self.start_dict = {
            SCHEDULERS_SERVICE: lambda sync: self.yt_instance.start_schedulers(sync=sync, custom_paths=self.start_custom_paths),
            CONTROLLER_AGENTS_SERVICE: lambda sync: self.yt_instance.start_controller_agents(sync=sync, custom_paths=self.start_custom_paths),
            NODES_SERVICE: self.yt_instance.start_nodes,
            CHAOS_NODES_SERVICE: self.yt_instance.start_chaos_nodes,
            MASTERS_SERVICE: lambda sync: self.yt_instance.start_all_masters(
                start_secondary_master_cells=True, set_config=False, sync=sync),
            MASTER_CACHES_SERVICE: self.yt_instance.start_master_caches,
            QUEUE_AGENTS_SERVICE: self.yt_instance.start_queue_agents,
            RPC_PROXIES_SERVICE: self.yt_instance.start_rpc_proxy,
            HTTP_PROXIES_SERVICE: self.yt_instance.start_http_proxy,
            KAFKA_PROXIES_SERVICE: self.yt_instance.start_kafka_proxy,
        }
        self.kill_dict = {
            SCHEDULERS_SERVICE: lambda: self.yt_instance.kill_schedulers(*self.kill_args, **self.kill_kwargs),
            CONTROLLER_AGENTS_SERVICE: lambda: self.yt_instance.kill_controller_agents(*self.kill_args, **self.kill_kwargs),
            NODES_SERVICE: lambda: self.yt_instance.kill_nodes(*self.kill_args, **self.kill_kwargs),
            CHAOS_NODES_SERVICE: lambda: self.yt_instance.kill_chaos_nodes(*self.kill_args, **self.kill_kwargs),
            MASTERS_SERVICE: lambda: self.yt_instance.kill_all_masters(*self.kill_args, **self.kill_kwargs),
            MASTER_CACHES_SERVICE: lambda: self.yt_instance.kill_master_caches(*self.kill_args, **self.kill_kwargs),
            QUEUE_AGENTS_SERVICE: lambda: self.yt_instance.kill_queue_agents(*self.kill_args, **self.kill_kwargs),
            RPC_PROXIES_SERVICE: lambda: self.yt_instance.kill_rpc_proxies(*self.kill_args, **self.kill_kwargs),
            HTTP_PROXIES_SERVICE: lambda: self.yt_instance.kill_http_proxies(*self.kill_args, **self.kill_kwargs),
            KAFKA_PROXIES_SERVICE: lambda: self.yt_instance.kill_kafka_proxies(*self.kill_args, **self.kill_kwargs),
        }

    def __enter__(self):
        for comp_name in self.components:
            try:
                self.kill_dict[comp_name]()
            except KeyError:
                logger.error("Failed to kill {}. No such component.".format(comp_name))
                raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        for comp_name in self.components:
            try:
                self.start_dict[comp_name](sync=False)
                if self.sync:
                    self.yt_instance.synchronize()
            except KeyError:
                logger.error("Failed to start {}. No such component.".format(comp_name))
                raise


@contextlib.contextmanager
def push_front_env_path(path):
    """
    Run everything as if os.environ["PATH"] is prepended with #path.

    :type path: str
    :param path: value to prepend PATH with.
    """
    old_env_path = os.environ.get("PATH")
    if old_env_path is None:
        os.environ["PATH"] = path
    else:
        os.environ["PATH"] = os.path.pathsep.join([path, old_env_path])
    try:
        yield
    finally:
        if old_env_path is None:
            del os.environ["PATH"]
        else:
            os.environ["PATH"] = old_env_path


def find_cri_endpoint():
    endpoint = os.getenv("CONTAINER_RUNTIME_ENDPOINT", "unix:///run/containerd/containerd.sock")
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(endpoint.removeprefix("unix://"))
    except:  # noqa
        logger.exception("CRI connection {} failed".format(endpoint))
        endpoint = None
    finally:
        sock.close()
    return endpoint


def wait_for_dynamic_config_update(client, expected_config, instances_path, config_node_name="dynamic_config_manager"):
    instances = client.list(instances_path)

    if not instances:
        return

    def check():
        batch_client = client.create_batch_client()

        # COMPAT(gryzlov-ad): Remove this when bundle_dynamic_config_manager is in cluster_node orchid
        if instances_path == "//sys/cluster_nodes" and config_node_name == "bundle_dynamic_config_manager":
            if not client.exists("{0}/{1}/orchid/{2}".format(instances_path, instances[0], config_node_name)):
                return True

        responses = [
            batch_client.get("{0}/{1}/orchid/{2}".format(instances_path, instance, config_node_name))
            for instance in instances
        ]
        batch_client.commit_batch()

        for response in responses:
            if not response.is_ok():
                raise YtResponseError(response.get_error())

            output = response.get_result()

            if expected_config != output.get("applied_config"):
                return False

        return True

    wait(check, error_message="Dynamic config didn't become as expected in time", ignore_exceptions=True)
