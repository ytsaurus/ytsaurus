from yt.packages.six.moves import _thread as thread

from threading import Thread
import time
import os
import logging

logger = logging.getLogger("Yt.local")

def _sync_mode_finalize_func(environment, process, process_call_args):
    logger.error("Process run by command '{0}' is dead! Terminating local YT processes..."\
                 .format(" ".join(process_call_args)))
    thread.interrupt_main()

class YTCheckingThread(Thread):
    def __init__(self, environment, delay, timeout):
        super(YTCheckingThread, self).__init__()
        self.environment = environment
        self.delay = delay
        self.timeout = timeout
        self.daemon = True
        self.is_running = True
        self._start_time = None

    def run(self):
        self._start_time = time.time()

        while self.is_running:
            timeout_occured = self.timeout is not None and time.time() - self._start_time > self.timeout
            if not os.path.exists(self.environment.pids_filename) or timeout_occured:
                thread.interrupt_main()
                break
            self.environment.check_liveness(callback_func=_sync_mode_finalize_func)
            time.sleep(self.delay)

    def stop(self):
        self.is_running = False
        self.join()

