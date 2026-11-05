import random
import tabulate
import threading
import time

from yt.wrapper import YtError
from dataclasses import dataclass

from .log import logger, TraceContext


@dataclass
class ThreadStatistics:
    successful: int = None
    failed: int = None
    custom_message: str = None
    id: str = None
    name: str = None

    def __add__(self, other):
        assert self.id == other.id
        return ThreadStatistics(
            self.successful + other.successful,
            self.failed + other.failed,
            id=self.id,
            name=self.name)


class ThreadWrapper:
    def run(
        self, finished_event: threading.Event, quit_event: threading.Event,
        *args, name=None, **kwargs
    ):
        self.quit_event = quit_event
        self.name = name

        def func(*args, **kwargs):
            try:
                self.do_run(*args, **kwargs)
            except Exception:
                logger.exception("Thread failed")
            finished_event.set()
            #  self.quit_event.wait()

        t = threading.Thread(target=func, args=args, kwargs=kwargs, name=name)
        t.start()
        return t

    def collect_statistics(self) -> ThreadStatistics | None:
        statistics = self.do_collect_statistics()
        if statistics is not None:
            statistics.name = self.name
            if statistics.id is None:
                statistics.id = statistics.name
        return statistics

    def do_collect_statistics(self) -> ThreadStatistics | None:
        pass


class Periodic:
    def __init__(self, period=0):
        self.last_stat_time = time.time()
        self.period = period
        super().__init__()

    def print_stats(self):
        pass

    def do_run(self, *args, **kwargs):
        delay = 0
        next_iteration_start_time = 0
        while not self.quit_event.is_set():
            if time.time() < next_iteration_start_time:
                time.sleep(0.1)
                continue
            with TraceContext() as trace:
                try:
                    self.do_run_once(*args, trace=trace, **kwargs)
                    delay = 0
                    if self.period > 0:
                        next_iteration_start_time = time.time() + self.period
                except (YtError, TimeoutError):
                    delay = min(delay + 1, 5)
                    logger.info(self.get_sleep_error_message(delay))
                    #  logger.debug(self.get_sleep_error_message(delay))
                    next_iteration_start_time = time.time() + delay + random.random() * 0.1
            if time.time() > self.last_stat_time + 30:
                self.print_stats()
                self.last_stat_time = time.time()


class MultiRunner:
    def __init__(self):
        self.finished_event = threading.Event()
        self.quit_event = threading.Event()
        self.threads: list[threading.Thread] = []
        self.targets: list[ThreadWrapper] = []
        self.statistics_period = 3
        self.next_statistics_print_time = time.time() + self.statistics_period
        self.total_statistics: dict[str, ThreadStatistics] = {}

    def start_thread(self, target: ThreadWrapper, *args, **kwargs):
        self.targets.append(target)
        self.threads.append(target.run(
            self.finished_event, self.quit_event, *args, **kwargs))

    def aggregate_statistics(self, statistics):
        result = {}
        for id, s in statistics:
            if id in result:
                result[id] += s
            else:
                result[id] = s
        return result

    def print_statistics(self):
        self.next_statistics_print_time = time.time() + self.statistics_period

        statistics = []
        custom_messages = []
        for target in self.targets:
            if (s := target.collect_statistics()) is not None:
                statistics.append(s)
                if (m := s.custom_message) is not None:
                    custom_messages.append(f"{s.id}: {m}")

        statistics = self.aggregate_statistics([(s.id, s) for s in statistics])
        self.total_statistics = self.aggregate_statistics(list(statistics.items()) + list(self.total_statistics.items()))

        table = [["Name", "Total", "OK", "Failed", "5s total", "5s OK", "5s failed"]]
        for s in sorted(self.total_statistics.keys()):
            total = self.total_statistics[s]
            row = [total.id, total.successful + total.failed, total.successful, total.failed]
            if s in statistics:
                current = statistics[s]
                row.extend([current.successful + current.failed, current.successful, current.failed])
            else:
                row.extend(["", "", ""])
            table.append(row)
        message = tabulate.tabulate(table, headers="firstrow") + "\n" + "\n".join(custom_messages)
        logger.info("Dumping statistics\n" + message)

    def wait(self, duration=None):
        start_time = time.time()

        gracefully_terminated = False

        try:
            while True:
                if duration is not None and time.time() > start_time + duration:
                    gracefully_terminated = True
                    break

                self.finished_event.wait(0.1)
                if self.finished_event.is_set():
                    break

                if time.time() > self.next_statistics_print_time:
                    self.print_statistics()

        except KeyboardInterrupt:
            self.finished_event.set()

        if gracefully_terminated:
            logger.info(f"Successfully ran for {duration} seconds, terminating gracefully")
        else:
            logger.info("Finished event is set, something went wrong, setting quit event")

        self.quit_event.set()
        for t in self.threads:
            t.join()
            logger.info("Joined thread")
        logger.info("Terminated")

        return gracefully_terminated
