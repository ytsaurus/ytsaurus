from yt.common import to_native_str, YtError, which

# COMPAT for tests.
try:
    from yt.test_helpers import (are_almost_equal, wait, unorderable_list_difference,
                                 assert_items_equal, WaitFailed, Counter)
    assert_almost_equal = are_almost_equal
except ImportError:
    pass

try:
    import yt.json_wrapper as json
except ImportError:
    import yt.json as json

import yt.yson as yson

from yt.packages.six import iteritems, PY3, text_type, Iterator
from yt.packages.six.moves import xrange, map as imap

import socket
import os
import fcntl
import random
import codecs
import logging
import errno
import time

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

logger = logging.getLogger("Yt.local")

class OpenPortIterator(Iterator):
    GEN_PORT_ATTEMPTS = 10
    START_PORT = 10000

    def __init__(self, port_locks_path=None):
        self.busy_ports = set()

        self.port_locks_path = port_locks_path
        self.lock_fds = set()

        self.local_port_range = None
        if os.path.exists("/proc/sys/net/ipv4/ip_local_port_range"):
            with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
                self.local_port_range = list(imap(int, f.read().split()))

    def release_locks(self):
        for lock_fd in self.lock_fds:
            try:
                os.close(lock_fd)
            except OSError as err:
                logger.warning("Failed to close file descriptor %d: %s",
                               lock_fd, os.strerror(err.errno))

    def __iter__(self):
        return self

    def __next__(self):
        for _ in xrange(self.GEN_PORT_ATTEMPTS):
            port = None
            if self.local_port_range is not None and \
                    self.local_port_range[0] - self.START_PORT > 1000:
                # Generate random port manually and check that it is free.
                port_value = random.randint(self.START_PORT, self.local_port_range[0] - 1)
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.bind(("", port_value))
                    sock.listen(1)
                    port = port_value
                except Exception:
                    pass
                finally:
                    sock.close()
            else:
                # Generate random local port by bind to 0 port.
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.bind(("", 0))
                    sock.listen(1)
                    port = sock.getsockname()[1]
                finally:
                    sock.close()

            if port is None:
                continue

            if port in self.busy_ports:
                continue

            if self.port_locks_path is not None:
                try:
                    lock_fd = os.open(os.path.join(self.port_locks_path, str(port)),
                                      os.O_CREAT | os.O_RDWR)
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except IOError:
                    if lock_fd != -1:
                        os.close(lock_fd)
                    self.busy_ports.add(port)
                    continue

                self.lock_fds.add(lock_fd)

            self.busy_ports.add(port)

            return port
        else:
            raise RuntimeError("Failed to generate open port after {0} attempts"
                               .format(self.GEN_PORT_ATTEMPTS))

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
            writer = lambda stream: stream
            if PY3:
                writer = codecs.getwriter("utf-8")
            json.dump(_fix_yson_booleans(config), writer(f), indent=4)
        elif format == "yson":
            yson.dump(config, f, yson_format="pretty", boolean_as_string=False)
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
            reader = lambda stream: stream
            if PY3:
                reader = codecs.getreader("utf-8")
            return json.load(reader(f))
        else:
            return to_native_str(f.read())

def is_dead_or_zombie(pid):
    try:
        with open("/proc/{0}/status".format(pid), "r") as f:
            for line in f:
                if line.startswith("State:"):
                    return line.split()[1] == "Z"
    except IOError:
        pass

    return True

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

def add_binary_path(relative_path):
    if yatest_common is None and "ARCADIA_PATH" not in os.environ:
        return

    if yatest_common is not None:
        binary_path = yatest_common.binary_path(relative_path)
    else:
        binary_path = os.path.join(os.environ["ARCADIA_PATH"], relative_path)

    if not which(os.path.basename(binary_path)):
        os.environ["PATH"] = os.path.dirname(binary_path) + ":" + os.environ["PATH"]
