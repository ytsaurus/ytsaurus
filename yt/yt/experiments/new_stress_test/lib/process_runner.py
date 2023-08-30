from .logger import logger

from yt.wrapper import YtError

import time
from datetime import datetime
from multiprocessing import Process, Queue, current_process

class ProcessState:
    error = None
    finished = False

    def __init__(self, process, queue, name):
        self.process: Process = process
        self.queue: Queue = queue
        self.name = name

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
                    except YtError as e:
                        # Some YtError-s are not picklable, so we transfer them as dict
                        # and store a flag to preserve the type.
                        queue.put((1, e.simplify()))
                        raise
                    except Exception as e:
                        queue.put((0, e))
                        raise

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

                process.process.join(10)
                if process.process.is_alive():
                    cur_time = int(time.mktime(datetime.now().timetuple()))
                    logger.info("Process %s (%s) is still running for %s sec" % (
                        process.process.pid, process.name, cur_time - process.process.start_time))
                    continue

                error = None
                if process.process.exitcode != 0:
                    error = Exception("Process has nonzero exit code")
                if not process.queue.empty():
                    logger.info("Getting from queue")
                    is_yt_error, error = process.queue.get()
                    logger.info("Got from queue")
                    if is_yt_error:
                        error = YtError.from_dict(error)
                process.error = error

                process.finished = True
                pending_count -= 1

        result = [process.error for process in self.processes]
        self.processes = []
        return result

process_runner = ProcessRunner()
