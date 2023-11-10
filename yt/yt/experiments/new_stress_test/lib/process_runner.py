from .logger import logger

from yt.wrapper import YtError

import time
from datetime import datetime
from multiprocessing import Process, Queue, current_process
import queue


class ProcessState:
    error = None
    finished = False

    def __init__(self, process, queue, name):
        self.process: Process = process
        self.queue: Queue = queue
        self.name = name


class ProcessResult:
    def __init__(self, error=None):
        if error is None:
            self.success = True
        else:
            self.success = False
            if isinstance(error, YtError):
                self.is_yt_error = True
                self.error = error.simplify()
            else:
                self.is_yt_error = False
                self.error = error

    def get_error(self):
        if self.success:
            return None
        elif self.is_yt_error:
            return YtError.from_dict(self.error)
        else:
            return self.error


class ProcessRunner:
    def __init__(self):
        self.processes: list[ProcessState] = []

    def run_in_process(self):
        def decorator(func):
            def wrapper(*args, **kwargs):
                def error_handling_wrapper(queue: Queue, args, kwargs):
                    try:
                        logger.pid = current_process().pid
                        logger.func = func.__name__
                        func(*args, **kwargs)
                        logger.info("Function finished successfully")
                    except Exception as e:
                        queue.put(ProcessResult(e))
                    else:
                        queue.put(ProcessResult())

                queue = Queue()
                process = Process(target=error_handling_wrapper, args=(queue, args, kwargs))
                process.start()
                process.start_time = int(time.mktime(datetime.now().timetuple()))
                logger.info("Running function %s in process %s" % (func.__name__, process.pid))
                self.processes.append(ProcessState(process, queue, func.__name__))
            return wrapper
        return decorator

    def join_processes(self):
        pending_count = len(self.processes)
        while pending_count > 0:
            for process in self.processes:
                if process.finished:
                    continue

                result = None
                try:
                    result = process.queue.get(timeout=1)
                except queue.Empty:
                    if process.process.is_alive():
                        cur_time = int(time.mktime(datetime.now().timetuple()))
                        logger.info("Process %s (%s) is still running for %s sec" % (
                            process.process.pid, process.name, cur_time - process.process.start_time))
                        continue

                    # In the rare case the process may have had finished (and put its outcome to the queue)
                    # after queue.Empty was raised but before it was caught. We check the queue once more.
                    # However, we should not block on it since the process may have been terminated for external
                    # reasons without putting anything to the queue.
                    try:
                        result = process.queue.get(timeout=0.1)
                    except:
                        pass

                process.process.join()
                error = result.get_error() if result is not None else None
                if error is None and process.process.exitcode != 0:
                    error = Exception("Process has nonzero exit code {}".format(
                        process.process.exitcode))
                process.error = error

                process.finished = True
                pending_count -= 1

        result = [process.error for process in self.processes]
        self.processes = []
        return result


process_runner = ProcessRunner()
