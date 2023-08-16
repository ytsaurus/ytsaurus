from .logger import logger

import time
from datetime import datetime
import multiprocessing

class ProcessRunner(object):
    def __init__(self):
        self.processes = []

    def run_in_process(self):
        def decorator(func):
            def wrapper(*args, **kwargs):
                process = multiprocessing.Process(target=func, args=args, kwargs=kwargs)
                process.start()
                process.start_time = int(time.mktime(datetime.now().timetuple()))
                logger.info("Running function %s in process %s" % (func.__name__, process.pid))
                self.processes.append(process)
            return wrapper
        return decorator

    def join_processes(self):
        success = True
        while len(self.processes) > 0:
            for process in self.processes:
                process.join(60)
                if process.is_alive():
                    cur_time = int(time.mktime(datetime.now().timetuple()))
                    logger.info("Process %s is still running for %s sec" % (process.pid, cur_time - process.start_time))
                elif process.exitcode != 0:
                    success = False

            self.processes = [process for process in self.processes if process.is_alive()]

        return success

process_runner = ProcessRunner()
