import yt.json as json
import yt.yson as yson

import socket
import os
import fcntl
import subprocess

GEN_PORT_ATTEMPTS = 10

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

def get_open_port(port_locks_path=None):
    for _ in xrange(GEN_PORT_ATTEMPTS):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("", 0))
            sock.listen(1)
            port = sock.getsockname()[1]
        finally:
            sock.close()

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
        return map(int, v.split("."))
    return cmp(normalize(version1), normalize(version2))

def is_binary_found(binary_name):
    for path in os.environ["PATH"].split(os.pathsep):
        if os.access(os.path.join(path, binary_name), os.X_OK):
            return True
    return False

def collect_events_from_logs(log_files, event_filters):
    all_events = []

    def filter_func(event):
        for event_filter in event_filters:
            if event_filter(event):
                return True
        return False

    for log in log_files:
        if not os.path.exists(log):
            all_events.append([])
            continue

        with open(log) as f:
            all_events.append(filter(filter_func, reversed(f.readlines())))

    return all_events

def _fix_yson_booleans(obj):
    if isinstance(obj, dict):
        for key, value in obj.items():
            _fix_yson_booleans(value)
            if isinstance(value, yson.YsonBoolean):
                obj[key] = True if value else False
    elif isinstance(obj, list):
        for value in obj:
            _fix_yson_booleans(value)
    return obj

def write_config(config, filename, format="yson"):
    with open(filename, "wt") as f:
        if format == "yson":
            yson.dump(config, f, yson_format="pretty", boolean_as_string=False)
        elif format == "json":
            json.dump(_fix_yson_booleans(config), f, indent=4)
        else:
            f.write(config)

def read_config(filename, format="yson"):
    with open(filename, "r") as f:
        if format == "yson":
            return yson.load(f)
        elif format == "json":
            return json.load(f)
        else:
            return f.read()

def is_dead_or_zombie(pid):
    try:
        with open("/proc/{0}/status".format(pid), "rb") as f:
            for line in f:
                if line.startswith("State:"):
                    return line.split()[1] == "Z"
    except IOError:
        pass

    return True

def get_lsof_diagnostic(port):
    command = "set -o pipefail; sudo lsof -i :{0} | sed 1d".format(port)
    try:
        lsof_output = subprocess.check_output(command, shell=True, executable="/bin/bash").strip()
        return "Failed to bind port {0}, lsof output: {1}" \
               .format(port, lsof_output)
    except subprocess.CalledProcessError:
        pass

    return "Failed to bind port {0}, lsof found nothing.".format(port)
