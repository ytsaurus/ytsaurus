import logging
import threading


logger = logging.getLogger(__name__)


class PollMonitor(object):
    def __init__(self, delay):
        self.delay = delay
        self.thread = None
        self.stop_event = threading.Event()

    def setup_monitor(self):
        pass

    def teardown_monitor(self):
        pass

    def poll(self):
        raise NotImplementedError()

    def start(self):
        assert not self.is_alive()
        self.setup_monitor()
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()

    def _run(self):
        try:
            self.poll()
            while self.stop_event.wait(self.delay) is False:
                self.poll()
        except Exception as e:
            logger.exception("%s monitor failed with: %s", self, e)

    def stop(self):
        if self.is_alive():
            self.stop_event.set()
            self.thread.join()
            self.thread = None
            self.teardown_monitor()

    def is_alive(self):
        return self.thread is not None
