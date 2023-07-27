import contextlib
import json
import logging
import six
import sys
import time
import ujson

import devtools.ya.yalibrary.streaming_json_dumper as dumper
import library.python.json
import yatest.common
from exts.decompress import udopen

TEST_FILE = "NUL" if sys.platform.lower().startswith('win') else "/dev/null"

logger = logging.getLogger(__name__)


def test_performance(metrics):
    json_path = yatest.common.runtime.work_path("graph.json.uc")
    with duration(metrics, "graph_load_time"):
        with udopen(json_path) as f:
            graph = library.python.json.loads(f.read(), intern_keys=True, intern_vals=True)

    with duration(metrics, "standard_dump_time"):
        standard_dump(graph)

    with duration(metrics, "ultrajson_dump_time") as ultrajson_time:
        ultrajson_dump(graph)

    with duration(metrics, "streaming_dump_time") as streaming_time:
        streaming_dump(graph)

    assert streaming_time() < ultrajson_time()


def standard_dump(graph):
    with open(TEST_FILE, "w") as f:
        json.dump(graph, f, separators=(',', ':'))


def ultrajson_dump(graph):
    assert isinstance(graph, dict)
    assert "graph" in graph and isinstance(graph["graph"], list)

    # Ultrajson doesn't support streaming so we need to iterate over json explicitly
    with open(TEST_FILE, "w") as f:
        need_dict_comma = False
        f.write("{")
        for k, v in six.iteritems(graph):
            if need_dict_comma:
                f.write(",")
            else:
                need_dict_comma = True
            f.write('"{}":'.format(k))
            if k == "graph":
                need_list_comma = False
                f.write("[")
                for node in v:
                    if need_list_comma:
                        f.write(",")
                    else:
                        need_list_comma = True
                    ujson.dump(node, f)
                f.write("]")
            else:
                ujson.dump(v, f)
        f.write("}")


def streaming_dump(graph):
    with open(TEST_FILE, "wb") as f:
        dumper.dump(graph, f)


@contextlib.contextmanager
def duration(metrics, metric_name):
    start = time.time()
    duration = 0

    yield lambda: duration

    duration = time.time() - start
    metrics.set(metric_name, duration)
    logger.debug("{}: {:.1f}".format(metric_name, duration))
