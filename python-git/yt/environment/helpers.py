from yt.common import to_native_str
import yt.json as json
import yt.yson as yson

from yt.packages.six import iteritems, PY3, text_type
from yt.packages.six.moves import xrange, map as imap

import socket
import os
import fcntl
import random
import codecs
import subprocess

GEN_PORT_ATTEMPTS = 10
START_PORT = 10000

try:
    from unittest.util import unorderable_list_difference
except ImportError:
    def unorderable_list_difference(expected, actual, ignore_duplicate=False):
        """Same behavior as sorted_list_difference but
        for lists of unorderable items (like dicts).

        As it does a linear search per item (remove) it
        has O(n*n) performance.
        """
        missing = []
        unexpected = []
        while expected:
            item = expected.pop()
            try:
                actual.remove(item)
            except ValueError:
                missing.append(item)
            if ignore_duplicate:
                for lst in expected, actual:
                    try:
                        while True:
                            lst.remove(item)
                    except ValueError:
                        pass
        if ignore_duplicate:
            while actual:
                item = actual.pop()
                unexpected.append(item)
                try:
                    while True:
                        actual.remove(item)
                except ValueError:
                    pass
            return missing, unexpected

        # anything left in actual is unexpected
        return missing, actual

try:
    from collections import Counter
except ImportError:
    def Counter(iterable):
        result = {}
        for item in iterable:
            result[item] = result.get(item, 0) + 1
        return result

def assert_items_equal(actual_seq, expected_seq):
    # It is simplified version of the same method of unittest.TestCase
    try:
        actual = Counter(iter(actual_seq))
        expected = Counter(iter(expected_seq))
    except TypeError:
        # Unsortable items (example: set(), complex(), ...)
        actual = list(actual_seq)
        expected = list(expected_seq)
        missing, unexpected = unorderable_list_difference(expected, actual)
    else:
        if actual == expected:
            return
        missing = list(expected - actual)
        unexpected = list(actual - expected)

    assert not missing, "Expected, but missing:\n    %s" % repr(missing)
    assert not unexpected, "Unexpected, but present:\n    %s" % repr(unexpected)

def assert_almost_equal(actual, expected, decimal_places=4):
    eps = 10**(-decimal_places)
    return abs(actual - expected) < eps

def get_open_port(port_locks_path=None):
    local_port_range = None
    if os.path.exists("/proc/sys/net/ipv4/ip_local_port_range"):
        local_port_range = list(imap(int, open("/proc/sys/net/ipv4/ip_local_port_range").read().split()))

    for _ in xrange(GEN_PORT_ATTEMPTS):
        port = None
        if local_port_range is not None and local_port_range[0] - START_PORT > 1000:
            # Generate random port manually and check that it is free.
            port_value = random.randint(START_PORT, local_port_range[0] - 1)
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

        if port in get_open_port.busy_ports:
            continue

        if port_locks_path is not None:
            try:
                lock_fd = os.open(os.path.join(port_locks_path, str(port)), os.O_CREAT | os.O_RDWR)
                fcntl.lockf(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError:
                if lock_fd != -1:
                    os.close(lock_fd)
                get_open_port.busy_ports.add(port)
                continue

            get_open_port.lock_fds.add(lock_fd)

        get_open_port.busy_ports.add(port)

        return port

    raise RuntimeError("Failed to generate random port")

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
