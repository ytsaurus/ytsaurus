#!/usr/bin/env python

import json
import logging
import os
import pycurl
import re
import sys
import time
import traceback
import weakref

from cStringIO import StringIO
from collections import deque

import signal
signal.signal(signal.SIGPIPE, signal.SIG_IGN)

LOG = logging.getLogger("YtCollector")
LOG.setLevel("INFO")
LOG.addHandler(logging.StreamHandler(sys.stderr))
LOG.handlers[0].setFormatter(logging.Formatter("%(asctime)-15s: %(message)s"))


CONCURRENCY = 6
TIMEOUT_REQUEST = 10
TIMEOUT_MULTI_REQUEST = 20
METRIC_PREFIX = "yt"
METRIC_REGEXP = re.compile(r"[^a-zA-Z0-9_/]")


assert sys.maxint.bit_length() == 63  # Otherwise, time calculations won't fit.


def aggregate_max(values):
    return max(values)


def aggregate_avg(values):
    return sum(values) / len(values)


def publish(name, value, timestamp=None, **kwargs):
    if timestamp is None:
        timestamp = int(time.time())
    result = "%s.%s" % (METRIC_PREFIX, name)
    result += " " + str(timestamp) + " " + str(value)
    result += " " + " ".join("%s=%s" % (k, v) for (k, v) in kwargs.iteritems())
    sys.stdout.write(result)
    sys.stdout.write("\n")
    sys.stdout.flush()


def publish_timing(wrapped):
    def wrapper(*args, **kwargs):
        now = time.time()
        try:
            return wrapped(*args, **kwargs)
        finally:
            pargs = ("collector.%s" % wrapped.func_name, time.time() - now)
            pfunc = args[0].publish if hasattr(args[0], "publish") else publish
            pfunc(*pargs)
    wrapper.func_name = wrapped.func_name
    return wrapper


class Request(object):
    def __init__(self, url=None, aux=None):
        self.stream = None
        self.handle = pycurl.Curl()
        self.handle.request = weakref.ref(self)  # Allow GC.
        self.handle.setopt(pycurl.FOLLOWLOCATION, 0)
        self.handle.setopt(pycurl.TIMEOUT, TIMEOUT_REQUEST)
        self.handle.setopt(pycurl.CONNECTTIMEOUT, TIMEOUT_REQUEST)
        self.handle.setopt(pycurl.NOSIGNAL, 1)
        if url:
            self.reset(url, aux)

    def reset(self, url, aux):
        self.url = url
        self.aux = aux
        self.stream = StringIO()
        self.handle.setopt(pycurl.URL, url)
        self.handle.setopt(pycurl.WRITEFUNCTION, self.stream.write)

    def perform(self):
        try:
            self.handle.perform()
            return self._finish()
        finally:
            self._cleanup()
            self._destroy()

    def _finish(self):
        try:
            error = self.handle.errstr()
            if error:
                raise RuntimeError(error)
            data = self.stream.getvalue()
            data = json.loads(data)
            return data
        finally:
            self._cleanup()

    def _cleanup(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        if self.url:
            self.url = None
        if self.aux:
            self.aux = None

    def _destroy(self):
        self.handle.close()
        self.handle = None


class MultiRequest(object):
    def __init__(self):
        self.multi = pycurl.CurlMulti()
        self.queue = deque()
        self.pool = deque(Request() for _ in xrange(CONCURRENCY))
        self.busy = set()

    def add(self, url, aux):
        self.queue.append((url, aux))

    def perform(self):
        try:
            now = time.time()
            done = 0
            total = len(self.queue)

            while done < total:
                # If there is a spare connection and an item in the queue,
                # match them and push into |self.multi|.
                while self.queue and self.pool:
                    url, aux = self.queue.popleft()
                    request = self.pool.popleft()
                    request.reset(url, aux)
                    self.multi.add_handle(request.handle)
                    self.busy.add(request)

                # Do any work available.
                while True:
                    rv, _ = self.multi.perform()
                    if rv == pycurl.E_CALL_MULTI_PERFORM:
                        continue
                    if rv == pycurl.E_OK:
                        break
                    raise RuntimeError("Unknown error while performing multi-request")

                # Check for any requests completed.
                while True:
                    sz, good_list, bad_list = self.multi.info_read()
                    done += len(good_list) + len(bad_list)
                    for handle in good_list:
                        request = handle.request()
                        yield request.aux, request._finish()
                        self.multi.remove_handle(handle)
                        self.busy.remove(request)
                        self.pool.append(request)
                    for handle, errno, errstr in bad_list:
                        request = handle.request()
                        yield request.aux, request._cleanup()
                        self.multi.remove_handle(handle)
                        self.busy.remove(request)
                        self.pool.append(request)
                    if sz == 0:
                        break

                # Dead man's switch.
                if time.time() - now > TIMEOUT_MULTI_REQUEST:
                    LOG.error("Timed out while performing multi-request")
                    self.pool.extend(self.busy)
                    self.busy.clear()
                    return

                self.multi.select(1 + self.multi.timeout())

        finally:
            self._destroy()

    def _destroy(self):
        for request in self.pool:
            request._cleanup()
            request._destroy()

        self.multi.close()
        self.multi = None


class YtMetric(object):
    def __init__(self, path, window):
        now = time.time()

        self.path = path
        self.name = METRIC_REGEXP.sub("_", self.path).replace("/", ".").lstrip(".")
        self.tail = []
        self.window = int(window)
        self.bucket = int(now) / self.window
        self.offset = self.get_offset(-1)

    def __str__(self):
        return self.name

    def get_timestamp(self, i=0):
        return int(self.window * (self.bucket + i))

    def get_offset(self, i=0):
        return int(self.window * (self.bucket + i) * 1E6)

    def update(self, data):
        for point in data:
            if self.offset > point["time"]:
                LOG.debug("Out-of-order data point encountered; ignoring...")
                continue
            else:
                self.offset = point["time"]

            while self.offset > self.get_offset(1):
                if len(self.tail) > 0:
                    timestamp = self.get_timestamp()
                    point_avg = aggregate_avg(self.tail)
                    point_max = aggregate_max(self.tail)
                    yield timestamp, point_avg, point_max
                    self.tail = []
                self.bucket += 1
            self.tail.append(point["value"])

    def build_query(self):
        return "?from_time=%s" % self.offset


class YtCollector(object):
    def __init__(self, host, port, window, sync_period):
        self.host = host
        self.port = port
        self.window = window
        self.sync_at = 0
        self.sync_period = sync_period

        self.service = None
        self.metrics = {}

        LOG.info("Registered new collector %s", self)

    def __str__(self):
        return str((self.host, self.port))

    def invoke(self):
        try:
            self.service = Request(self.build_url("/orchid/@service_name")).perform()
            self.invoke_unsafe()
        except pycurl.error as ex:
            LOG.error(
                "Failed to invoke collector %s: %s\n%s",
                self,
                str(ex),
                traceback.format_exc())

    def invoke_unsafe(self):
        now = time.time()
        if now - self.sync_at > self.sync_period:
            self.gather_metrics()

        self.gather_values()

    def publish(self, *args):
        publish(*args, service_name=self.service, service_port=self.port)

    @publish_timing
    def gather_metrics(self):
        def traverse(root, prefix):
            for key, child in root.iteritems():
                path = str(prefix + "/" + key)
                if isinstance(child, dict):
                    for subpath in traverse(child, path):
                        yield subpath
                elif isinstance(child, type(None)):
                    yield path
                else:
                    raise RuntimeError("Unknown object within profiling data")

        old_metrics = self.metrics
        new_metrics = {}

        data = Request(self.build_profiling_url()).perform()
        for path in traverse(data, ""):
            if path in old_metrics:
                LOG.debug("Keeping old metric %s", self.build_profiling_url(path))
                new_metrics[path] = old_metrics[path]
            else:
                LOG.debug("Discovered new metric %s", self.build_profiling_url(path))
                new_metrics[path] = YtMetric(path=path, window=self.window)

        self.publish("collector.metrics_count", len(new_metrics))
        self.metrics = new_metrics

    @publish_timing
    def gather_values(self):
        multi = MultiRequest()
        for metric in self.metrics.itervalues():
            url = self.build_profiling_url(metric.path, metric.build_query())
            multi.add(url, metric)

        points = 0
        for metric, data in multi.perform():
            for timestamp, point_avg, point_max in metric.update(data):
                points += 1
                self.publish(metric.name + ".avg", point_avg, timestamp)
                self.publish(metric.name + ".max", point_max, timestamp)

        self.publish("collector.values_count", points)

    def build_url(self, *args):
        return "http://%s:%s%s" % (self.host, self.port, "".join(args))

    def build_profiling_url(self, *args):
        return self.build_url("/orchid/profiling", *args)


if __name__ == "__main__":
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "etc", "yt_collector.json")
        config_path = os.path.normpath(config_path)
        config = json.load(open(config_path, "r"))
    except IOError:
        config = {"window": 5, "sync_period": 600, "interval": 15, "sources": []}

    window = int(config["window"])
    sync_period = int(config["sync_period"])
    interval = int(config["interval"])
    sources = list(config["sources"])

    collectors = []
    for source in sources:
        try:
            host, port = str(source).split(":", 1)  # Coerce from unicore to str
            collector = YtCollector(host, port, window, sync_period)
            collectors.append(collector)
        except KeyboardInterrupt:
            raise
        except Exception as ex:
            LOG.error(
                "Failed to register collector from source %r: %s\n%s",
                source,
                str(ex),
                traceback.format_exc())

    while True:
        for collector in collectors:
            collector.invoke()
        time.sleep(interval)
