#!/usr/bin/python -u

import atexit
import sys
import time
import json
import subprocess
import hashlib
import os

sys.stdout = os.fdopen(sys.stdout.fileno(), "w", 0)
sys.stderr = os.fdopen(sys.stderr.fileno(), "w", 0)

OPTS = {
    "src" : {
        "prefix"    : [],
        "yt_binary" : "/home/sandello/yt.testing",
        "yt_config" : "/home/sandello/build/ytdriver.conf"
    },
    "dst" : {
        "prefix"    : ["backup"],
        "yt_binary" : "/home/sandello/build/bin/yt",
        "yt_config" : "/home/sandello/build/ytdriver-dev.conf"
    }
}

class Statistics(object):
    def __init__(self):
        self.__lock = threading.Lock()
        self.bytes = 0L
        self.seconds = 0.0

    def update(self, bytes, seconds):
        self.__lock.acquire()
        self.bytes += long(bytes)
        self.seconds += float(seconds)
        self.__lock.release()

    def mibps(self):
        if self.seconds > 1e-7:
            return float(self.bytes) / float(self.seconds) / 1024.0 / 1024.0
        else:
            return 0.0

from Queue import Queue
from threading import Thread

class Worker(Thread):
    def __init__(self, tasks):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()
    
    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            try: func(*args, **kwargs)
            except Exception, e: print e
            self.tasks.task_done()

class ThreadPool:
    def __init__(self, number_of_threads):
        self.tasks = Queue(number_of_threads)
        for _ in range(number_of_threads): Worker(self.tasks)

    def add_task(self, func, *args, **kwargs):
        self.tasks.put((func, args, kwargs))

    def wait_completion(self):
        self.tasks.join()

WORKER_POOL = None

LOCAL_STATISTICS = Statistics()
REMOTE_STATISTICS = Statistics()

def flatten(x):
    return sum(map(flatten, x), []) if hasattr(x, "__iter__") else [x]

def remove_prefix(p, x):
    i = 0
    while i < len(p) and i < len(x) and p[i] == x[i]: i += 1
    return x[i:]

def hash_file(path):
    sha1 = hashlib.sha1()
    with open(path, "rb") as handle:
        sha1.update(handle.read())
    return sha1.hexdigest()

def shell_quote(s):
    return "'" + s.replace("'", "'\\''") + "'"

def ypath_escape(token):
    return "\"" + token + "\""

def ypath_join(*tokens):
    return "/" + "".join("/" + ypath_escape(token) for token in flatten(tokens))

def spawn_yt(which, command, *args, **kwargs):
    execv = [
        OPTS[which]["yt_binary"], command,
        "--config", OPTS[which]["yt_config"],
        "--format", "json"
    ] + list(args)
    execa = { "shell" : False, "stdout" : subprocess.PIPE, "stderr" : subprocess.PIPE }
    execa.update(kwargs)

    #print "Running", repr(execv)

    return subprocess.Popen(execv, **execa)

def ask_yt(which, command, *args, **kwargs):
    stdout, stderr = spawn_yt(which, command, *args, **kwargs).communicate()
    return json.loads(stdout) if stdout else None

def traverse_cypress(which, path_tokens=[]):
    if len(path_tokens) > 0 and (path_tokens[0] == "tmp" or path_tokens[0] == "sys"):
        return

    for directory in sorted(ask_yt(which, "list", ypath_join(path_tokens))):
        new_path_tokens = path_tokens + [directory]
        new_path_attributes = ask_yt(which, "get", ypath_join(new_path_tokens) + "/@")

        assert "type" in new_path_attributes

        if new_path_tokens[0] == "tmp" or new_path_tokens[0] == "sys":
            continue

        yield new_path_attributes["type"], new_path_tokens
        if new_path_attributes["type"] == "map_node":
            for item in traverse_cypress(which, new_path_tokens):
                yield item

def build_migration_plan(migrate_from="src", migrate_to="dst"):
    print "*" * 80
    print "*** Building migration plan..."
    print "*" * 80

    plan = []

    st = time.time()
    for ctype, cpath in traverse_cypress(migrate_from, OPTS[migrate_from]["prefix"]):
        new_cpath = OPTS[migrate_to]["prefix"] + remove_prefix(OPTS[migrate_from]["prefix"], cpath)
        plan.append((ctype, cpath, new_cpath))
    dt = time.time() - st

    number_of_files  = sum(1 for t, x, y in plan if t == "file" )
    number_of_tables = sum(1 for t, x, y in plan if t == "table")
    number_of_nodes  = len(plan) - number_of_files - number_of_tables

    print "*" * 80
    print "*** Migration plan was built in %.2fs (%d files, %d tables, %d nodes)" % \
        (dt, number_of_files, number_of_tables, number_of_nodes)
    print "*" * 80

    return plan

def prepare_migration_plan(plan, migrate_from="src", migrate_to="dst"):
    print "*" * 80
    print "*** Preparing for migration..."
    print "*" * 80

    st = time.time()

    tmp_prefix = "migrate_" + str(int(time.time())) + "_"

    tmp_binary = ypath_join("tmp", tmp_prefix + hash_file(OPTS[migrate_to]["yt_binary"]))
    tmp_config = ypath_join("tmp", tmp_prefix + hash_file(OPTS[migrate_to]["yt_config"]))
    tmp_sink   = ypath_join("tmp", tmp_prefix + "sink")

    OPTS[migrate_from]["migrator_binary"] = tmp_binary
    OPTS[migrate_from]["migrator_config"] = tmp_config
    OPTS[migrate_from]["migrator_sink"  ] = tmp_sink

    atexit.register(lambda: ask_yt(migrate_from, "remove", tmp_binary))
    with open(OPTS[migrate_to]["yt_binary"], "r") as handle:
        uploader = spawn_yt(migrate_from, "upload", tmp_binary, stdin=handle)
        uploader.communicate()

    ask_yt(migrate_from, "set", tmp_binary + "/@executable", "\"true\"")
    ask_yt(migrate_from, "set", tmp_binary + "/@file_name", "\"migrator_binary\"")

    atexit.register(lambda: ask_yt(migrate_from, "remove", tmp_config))
    with open(OPTS[migrate_to]["yt_config"], "r") as handle:
        uploader = spawn_yt(migrate_from, "upload", tmp_config, stdin=handle)
        uploader.communicate()
    ask_yt(migrate_from, "set", tmp_config + "/@executable", "\"false\"")
    ask_yt(migrate_from, "set", tmp_config + "/@file_name", "\"migrator_config\"")

    atexit.register(lambda: ask_yt(migrate_from, "remove", tmp_sink))
    ask_yt(migrate_from, "create", "table", tmp_sink)

    dt = time.time() - st

    print "*" * 80
    print "*** Preparation was done in %.2fs" % dt
    print "*" * 80

def execute_migration_plan(plan, migrate_from="src", migrate_to="dst"):
    migrators = {
        "file"     : migrate_file,
        "map_node" : migrate_map_node,
        "table"    : migrate_table
    }

    print "*" * 80
    print "*** Executing migration plan..."
    print "*" * 80

    st = time.time()
    for ctype, from_path, to_path in plan:
        print "-" * 80
        print " " * 3, "Migrating " + ctype
        print " " * 3, "  from " + ypath_join(from_path)
        print " " * 3, "    to " + ypath_join(to_path)
        print
        migrators[ctype](from_path, to_path, migrate_from, migrate_to)
        print
    dt = time.time() - st

    print "*" * 80
    print "*** Migration plan was executed in %.2fs" % dt
    print "*" * 80

def copy_attributes(attributes, from_path, to_path, migrate_from, migrate_to):
    for attribute in attributes:
        reader = spawn_yt(migrate_from,
            "get", ypath_join(from_path) + "/@" + attribute, stdout=subprocess.PIPE)
        writer = spawn_yt(migrate_to,
            "set", ypath_join(to_path)   + "/@" + attribute, stdin=reader.stdout)

        stdout, stderr = writer.communicate()

def migrate_file(from_path, to_path, migrate_from, migrate_to):
    reader = spawn_yt(migrate_from, "download", ypath_join(from_path), stdout=subprocess.PIPE)
    writer = spawn_yt(migrate_to,   "upload",   ypath_join(to_path),   stdin=reader.stdout)

    st = time.time()
    stdout, stderr = writer.communicate()
    dt = time.time() - st

    copy_attributes([ "executable", "file_name" ], from_path, to_path, migrate_from, migrate_to)

    LOCAL_STATISTICS.update(
        ask_yt(migrate_from, "get", ypath_join(from_path) + "/@size"),
        dt)

    print " " * 3, "Done in %.2fs" % dt

def migrate_map_node(from_path, to_path, migrate_from, migrate_to):
    ask_yt(migrate_to, "create", "map_node", ypath_join(to_path))

    print " " * 3, "Done"

def migrate_table(from_path, to_path, migrate_from, migrate_to):
    if ask_yt(migrate_from, "get", ypath_join(from_path) + "/@row_count") != ask_yt(migrate_to, "get", ypath_join(to_path) + "/@row_count"):
        print "Row count mismatch; removing target table"
        ask_yt(migrate_to, "remove", ypath_join(to_path))
    else:
        print "Row count match; skipping source table"
        return

    ask_yt(migrate_to, "create", "table", ypath_join(to_path))

    copy_attributes([ "channels" ], from_path, to_path, migrate_from, migrate_to)

    WORKER_POOL.add_task(migrate_table_inner, (from_path, to_path, migrate_from, migrate_to), {})

    print " " * 3, "Enqueued a worker task"

def migrate_table_inner(from_path, to_path, migrate_from, migrate_to):
    st = time.time()
    ask_yt(migrate_from, "map",
        "--file", OPTS[migrate_from]["migrator_binary"],
        "--file", OPTS[migrate_from]["migrator_config"],
        "--in", ypath_join(from_path), "--out", OPTS[migrate_from]["migrator_sink"],
        "--mapper", "{0} write --config {1} {2}".format(
            "./migrator_binary",
            "./migrator_config",
            shell_quote(ypath_join(to_path))),
        "--opt", "/spec/job_count=100",
        stdout=sys.stdout, stderr=sys.stdout)
    dt = time.time() - st

    REMOTE_STATISTICS.update(
        ask_yt(migrate_from, "get", ypath_join(from_path) + "/@uncompressed_data_size"),
        dt)

def main():
    st = time.time()

    WORKER_POOL = ThreadPool(5)

    plan = build_migration_plan()
    prepare_migration_plan(plan)
    execute_migration_plan(plan)

    WORKER_POOL.wait_completion()

    dt = time.time() - st

    print
    print "Migrated in %.2fs" % dt
    print
    print " Local statistics: %d bytes transferred in %.2fs (%.3f MiB/s)" % \
        (LOCAL_STATISTICS.bytes, LOCAL_STATISTICS.seconds, LOCAL_STATISTICS.mibps())
    print "Remote statistics: %d bytes transferred in %.2fs (%.3f MiB/s)" % \
        (REMOTE_STATISTICS.bytes, REMOTE_STATISTICS.seconds, REMOTE_STATISTICS.mibps())

if __name__ == "__main__":
    main()
