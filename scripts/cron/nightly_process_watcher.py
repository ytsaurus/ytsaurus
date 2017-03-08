#!/usr/bin/python2

from yt.packages.six.moves import xrange
from yt.wrapper.transaction import null_transaction_id
from yt.common import set_pdeathsig

import yt.logger as logger

import yt.wrapper as yt

import os
import time
import socket
import random
import signal
import argparse
import subprocess
from multiprocessing import Process, Queue

class BoundProcess(Process):
    def __init__(self, group=None, target=None, name=None, terminate=None, args=None, kwargs=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        self.__args = args
        self.__pid = os.getpid()
        self.__kwargs = kwargs
        self.__target = target

        super(BoundProcess, self).__init__(
            group=group,
            target=self.run,
            name=name,
            args=args,
            kwargs=kwargs)

    def run(self):
        set_pdeathsig()
        try:
            self.__target(*self.__args, **self.__kwargs)
        except:
            logger.exception("Process failed")
            self.__kill_parent()

    def __kill_parent(self):
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
            try:
                os.kill(self.__pid, sig)
            except OSError:
                break

            time.sleep(0.25)

def take_lock(queue, watcher_root):
    node_to_lock = yt.ypath_join(watcher_root, "lock")

    client = yt.YtClient(config=yt.config.config)
    client.create("map_node", node_to_lock, recursive=True, ignore_existing=True)

    with client.Transaction() as tx:
        logger.info("Acquiring lock under tx: %s", tx.transaction_id)
        while True:
            try:
                client.lock(node_to_lock, mode="exclusive")
                break
            except yt.YtResponseError as err:
                if err.is_concurrent_transaction_lock_conflict():
                    logger.info("Failed to take lock")
                    time.sleep(1.0)
                    continue
                logger.exception("Error occured during locking")
                raise

        logger.info("Lock acquired (tx: %s)", tx.transaction_id)

        with client.Transaction(transaction_id=null_transaction_id):
            client.set(watcher_root + "/@pid", str(os.getpid()))
            client.set(watcher_root + "/@host", socket.getfqdn())

        queue.put(True)

        while True:
            time.sleep(1.0)

class Watcher(object):
    SLEEP_DELAY = 3.0

    def __init__(self, worker_count, worker_command, worker_log_path,
                 tasks_root, watcher_root, collector_command,
                 collector_period, collector_log_path):
        self._worker_count = worker_count
        self._worker_command = worker_command
        self._worker_log_path = worker_log_path
        self._tasks_root = tasks_root
        self._watcher_root = watcher_root
        self._collector_command = collector_command
        self._collector_period = collector_period
        self._collector_log_path = collector_log_path

        self._processes = {}

        # Collector
        self._collector_process = None
        self._last_collector_start_time = None
        self._warn_if_collector_is_running = True

    def run_forever(self):
        self._spawn_workers()

        while True:
            self._manage_workers()
            self._update_alive_workers_attribute()
            self._update_current_task_count()
            self._manage_collector()

            time.sleep(self.SLEEP_DELAY)

    def _spawn_workers(self):
        for _ in xrange(self._worker_count):
            worker_id, process = self._spawn_new_worker_process()
            self._processes[worker_id] = process

        time.sleep(self.SLEEP_DELAY)

        # Checking that processes are still alive
        exited_process_count = 0
        for p in self._processes.itervalues():
            if p.poll() is not None:
                exited_process_count += 1

        if exited_process_count == self._worker_count:
            raise yt.YtError("All workers died after {:.2f}s".format(self.SLEEP_DELAY))

    def _manage_workers(self):
        workers_to_remove = []

        for worker_id, process in self._processes.iteritems():
            if process.poll() is not None:
                logger.warning("Worker (id: %s) exited with exit code %d, replacing it",
                               worker_id, process.returncode)
                workers_to_remove.append(worker_id)

        for _ in xrange(len(workers_to_remove)):
            new_worker_id, process = self._spawn_new_worker_process()
            self._processes[new_worker_id] = process

        for worker_id in workers_to_remove:
            del self._processes[worker_id]

    def _manage_collector(self):
        collector_is_running = self._collector_process is not None and \
            self._collector_process.poll() is None

        if not collector_is_running and self._collector_process is not None:
            if self._collector_process.poll() != 0:
                raise yt.YtError("Collector process exited with non-zero exit code {0}"
                                 .format(self._collector_process.returncode))
            else:
                logger.info("Collector process finished successfully")

            self._collector_process = None

        if self._last_collector_start_time is None or \
            time.time() - self._last_collector_start_time > self._collector_period:
                if collector_is_running and self._warn_if_collector_is_running:
                    # Collector is still running
                    logger.warning("Skipped new collector process start: old collector "
                                   "process is still running (pid: %d)", self._collector_process.pid)
                    self._warn_if_collector_is_running = False
                    return

                self._spawn_new_collector_process()
                self._warn_if_collector_is_running = True

    def _update_alive_workers_attribute(self):
        yt.set_attribute(self._tasks_root, "alive_workers", self._processes.keys())

    def _update_current_task_count(self):
        total_count = 0
        for key in self._processes.keys():
            try:
                total_count += yt.get_attribute(yt.ypath_join(self._tasks_root, key), "count")
            except yt.YtResponseError as err:
                if err.is_concurrent_transaction_lock_conflict():
                    continue  # some process is currently extracting task
                else:
                    raise

        yt.set_attribute(self._tasks_root, "task_count", total_count)

    def _spawn_new_collector_process(self):
        process = subprocess.Popen([
            self._collector_command,
            "--tasks-root", self._tasks_root
        ], preexec_fn=set_pdeathsig, stderr=open(self._collector_log_path, "a"))

        if process.poll() is not None and process.returncode != 0:
            raise yt.YtError("Collector process failed with non-zero exit code " + str(process.returncode))

        self._collector_process = process
        self._last_collector_start_time = time.time()

        logger.info("Started collector process, pid: %d", process.pid)

    def _spawn_new_worker_process(self):
        worker_id = "{:x}".format(random.randint(0, 2 ** 32 - 1)).zfill(8)
        tasks_path = yt.ypath_join(self._tasks_root, worker_id)
        yt.create("list_node", tasks_path, ignore_existing=True)

        process = subprocess.Popen([
            self._worker_command,
            "--id", worker_id,
            "--tasks-path", tasks_path
        ], preexec_fn=set_pdeathsig, stderr=open(self._worker_log_path, "a"))

        logger.info("Started worker process, id: %s, pid: %d", worker_id, process.pid)

        return worker_id, process

def main():
    parser = argparse.ArgumentParser(description="Runs and watches workers")
    # Worker settings
    parser.add_argument("--worker-command", required=True, help="worker command")
    parser.add_argument("--worker-count", type=int, required=True, help="how many workers to keep")
    parser.add_argument("--worker-log-path", required=True,
                        help="path to log for all workers")
    # Watcher settings
    parser.add_argument("--tasks-root", required=True,
                        help="root path for all worker processes tasks")
    parser.add_argument("--watcher-root", required=True,
                        help="root path for watcher stuff")
    # Collector settings
    parser.add_argument("--collector-command", required=True,
                        help="command to run periodically to update worker tasks")
    parser.add_argument("--collector-period", required=True, type=int,
                        help="period for collector script (seconds)")
    parser.add_argument("--collector-log-path", required=True,
                        help="path to collector log")

    args = parser.parse_args()

    logger.info("Nightly process watcher started")

    yt.create("map_node", args.watcher_root, recursive=True, ignore_existing=True)
    yt.create("map_node", args.tasks_root, recursive=True, ignore_existing=True)

    queue = Queue()
    lock_process = BoundProcess(target=take_lock, args=(queue, args.watcher_root))
    lock_process.daemon = True
    lock_process.start()

    queue.get()

    # Removing old tasks
    for task_list in yt.list(args.tasks_root):
        path = yt.ypath_join(args.tasks_root, task_list)
        try:
            yt.remove(path, recursive=True)
        except yt.YtResponseError as err:
            if err.is_concurrent_transaction_lock_conflict():
                logger.warning('Failed to remove tasks list: "%s", list is locked')
            else:
                raise

    logger.info("Removed all old tasks")

    watcher = Watcher(**dict(vars(args)))
    watcher.run_forever()

if __name__ == "__main__":
    main()
