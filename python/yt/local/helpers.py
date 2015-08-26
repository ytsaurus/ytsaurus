from threading import Thread

import thread
import time
import os
import logging

logger = logging.getLogger("Yt.local")

def _sync_mode_finalize_func(environment, process_call_args):
    logger.error("Process run by command '{0}' is dead! Terminating local YT processes..."\
                 .format(" ".join(process_call_args)))
    thread.interrupt_main()

class YTCheckingThread(Thread):
    def __init__(self, environment, delay):
        super(YTCheckingThread, self).__init__()
        self.environment = environment
        self.delay = delay
        self.daemon = True
        self.is_running = True

    def run(self):
        while self.is_running:
            if not os.path.exists(self.environment.pids_filename):
                logger.info("Local YT with id {0} was stopped".format(self.environment.id))
                thread.interrupt_main()
                break
            self.environment.check_liveness(callback_func=_sync_mode_finalize_func)
            time.sleep(self.delay)

    def stop(self):
        self.is_running = False
        self.join()

