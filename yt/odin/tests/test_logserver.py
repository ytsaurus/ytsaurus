from yt_odin.logserver import (
    run_logserver,
    OdinSocketHandler,
    OdinCheckSocketHandler,
    TERMINATED_STATE,
    UNKNOWN_STATE
)

from yt_odin.logging import TaskLoggerAdapter
from yt_odin.storage.storage import StorageForCluster
from yt_odin.test_helpers import generate_unique_id, wait

import logging
import os
import pickle
import struct
import time
import tempfile
from multiprocessing import Process


class DummyStorage(StorageForCluster):
    def __init__(self, storage_file=None, delimiter="\n", *args, **kwarg):
        self.storage_file = storage_file
        self.delimiter = delimiter

    def add_record(self, *args, **kwargs):
        print("Dummy storage: add record", args, kwargs)
        if self.storage_file is not None:
            with open(self.storage_file, "a") as f:
                f.write(kwargs["messages"] + self.delimiter)
            print("Record written to '%s'" % self.storage_file)

    def get_records(self, *args, **kwargs):
        return []


class FailingStorage(DummyStorage):
    def add_record(self, *args, **kwargs):
        super(FailingStorage, self).add_record(*args, **kwargs)
        raise RuntimeError("This storage failed!")


class TestContext(object):
    def __init__(self, storage, messages_max_size=None, die_on_error=True):
        self.storage = storage
        self.messages_max_size = messages_max_size
        self.die_on_error = die_on_error

    def __enter__(self):
        self.socket_path = os.path.join(tempfile.gettempdir(), "{}.sock".format(generate_unique_id("test_logserver")))
        max_write_batch_size = 256
        self.logserver_process = Process(target=run_logserver,
                                         args=(self.socket_path, self.storage, max_write_batch_size,
                                               self.messages_max_size, self.die_on_error))
        self.logserver_process.start()
        return self

    def __exit__(self, type, value, traceback):
        self.logserver_process.terminate()


class DummyService(object):
    def __init__(self, name, task_id, messages, socket_path, delay=0):
        self.name = name
        self.task_id = task_id
        self.messages = messages
        self.socket_path = socket_path
        self.delay = delay

    def run(self):
        runner_handler = OdinSocketHandler(self.socket_path)
        runner_logger = logging.Logger("runner {}".format(self.task_id))
        runner_logger.addHandler(runner_handler)
        runner_logger.setLevel(logging.DEBUG)
        runner_adapter = TaskLoggerAdapter(runner_logger, self.task_id)
        runner_handler.send_control_message(task_id=self.task_id, start_timestamp=0, service=self.name,
                                            state=UNKNOWN_STATE)
        runner_adapter.info("Starting check")

        task_handler = OdinCheckSocketHandler(self.socket_path, self.task_id)
        task_logger = logging.Logger("service.{}".format(self.task_id))
        task_logger.addHandler(task_handler)
        for msg in self.messages:
            task_logger.error(msg)
            if self.delay > 0:
                time.sleep(self.delay)

        runner_adapter.info("Check is finished")
        runner_handler.send_control_message(task_id=self.task_id, state=TERMINATED_STATE)


def run_services(services, may_fail=False):
    processes = [Process(target=service.run) for service in services]
    for p in processes:
        p.start()
    for p in processes:
        p.join()
        if not may_fail:
            assert p.exitcode == 0


def test_logserver(tmpdir):
    storage_file = str(tmpdir.join("storage.txt"))
    task_ids = [
        "deadbeef",
        "ololohaha",
        "abracadabra",
    ]

    with TestContext(DummyStorage(storage_file=storage_file)) as context:
        services = [
            DummyService(name="monty", task_id=task_ids[0], messages=["Hey", "I'm here"],
                         socket_path=context.socket_path),
            DummyService(name="vader", task_id=task_ids[1], messages=["I am your father!"],
                         socket_path=context.socket_path),
            DummyService(name="mike", task_id=task_ids[2], messages=["first", "second", "third"],
                         socket_path=context.socket_path),
        ]
        run_services(services)
        time.sleep(2)
        assert context.logserver_process.is_alive()  # nothing failed in log server

    with open(storage_file, "r") as f:
        lines = f.readlines()

    lines_per_task = {task_id: [] for task_id in task_ids}
    for line in lines:
        if not line.strip():
            continue
        matching_ids = [task_id for task_id in task_ids if task_id in line]
        assert len(matching_ids) == 1
        lines_per_task[matching_ids[0]].append(line)

    for task_lines in lines_per_task.values():
        assert "Starting check" in task_lines[0]
        assert "Check is finished" in task_lines[-1]
        assert len(task_lines) > 2


def test_logserver_truncation(tmpdir):
    MESSAGES_MAX_SIZE = 512
    DELIMITER = "\n-----------\n"

    storage_file = str(tmpdir.join("storage.txt"))
    storage = DummyStorage(storage_file, DELIMITER)

    with TestContext(storage, MESSAGES_MAX_SIZE) as context:
        services = [
            DummyService(name="lol", task_id="1", messages=["Is it because I'm long? " * 1000],
                         socket_path=context.socket_path),
            DummyService(name="olo", task_id="2", messages=["trololololo"] * 1000,
                         socket_path=context.socket_path),
        ]
        run_services(services)
        time.sleep(2)
        assert context.logserver_process.is_alive()  # nothing failed in log server

    with open(storage_file, "r") as f:
        records = f.read().split(DELIMITER)

    for record in records:
        assert 0 <= len(record) < MESSAGES_MAX_SIZE + 20


def test_logserver_malformed_record():
    for die_on_error in (True, False):
        with TestContext(DummyStorage(), die_on_error=die_on_error) as context:
            log_record = dict(msg="Read me if you can!")
            data = pickle.dumps(log_record)
            message = struct.pack(">L", len(data) + 200) + data
            handler = OdinSocketHandler(context.socket_path)
            handler.send(message)
            handler.close()

            time.sleep(20)
            if die_on_error:
                assert not context.logserver_process.is_alive()  # log server crashed, not hung
            else:
                assert context.logserver_process.is_alive()  # log server keeps serving


def test_logserver_failed_write(tmpdir):
    storage_file = str(tmpdir.join("storage.txt"))
    storage = FailingStorage(storage_file)

    with TestContext(storage, die_on_error=False) as context:
        services = [
            DummyService(name="joseph", task_id="111111",
                         messages=["Hello"] * 3,
                         socket_path=context.socket_path,
                         delay=0.5)
        ]
        run_services(services, may_fail=True)
        wait(lambda: os.stat(storage_file).st_size != 0, ignore_exceptions=True)
        run_services(services, may_fail=True)
        wait(lambda: not context.logserver_process.is_alive())
