import _thread as thread
from threading import Thread
import time
import os
import logging

logger = logging.getLogger("YtLocal")


def _sync_mode_finalize_func(environment, process, process_call_args):
    logger.error("Process run by command '{}' is exited with code {}. Terminating local YT processes..."
                 .format(" ".join(process_call_args), process.returncode))
    thread.interrupt_main()


class YTCheckingThread(Thread):
    def __init__(self, environment, delay, timeout, components_to_not_watch=None):
        super(YTCheckingThread, self).__init__()
        self.environment = environment
        self.delay = delay
        self.timeout = timeout
        self.components_to_not_watch = components_to_not_watch
        self.daemon = True
        self.is_running = True
        self._start_time = None

    def run(self):
        self._start_time = time.time()

        while self.is_running:
            timeout_occurred = self.timeout is not None and time.time() - self._start_time > self.timeout
            if not os.path.exists(self.environment.pids_filename) or timeout_occurred:
                thread.interrupt_main()
                break
            self.environment.check_liveness(
                callback_func=_sync_mode_finalize_func,
                ignored_components=self.components_to_not_watch,
            )
            time.sleep(self.delay)

    def stop(self):
        self.is_running = False
        self.join()
