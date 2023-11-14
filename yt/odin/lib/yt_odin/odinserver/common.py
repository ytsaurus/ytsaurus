import os
import datetime
import logging
import signal
import time
import dateutil.parser
from copy import deepcopy
from multiprocessing import Process

odin_logger = logging.getLogger("Odin")

MINUTES_IN_DAY = 1440
SECONDS_IN_DAY = 86400
SECONDS_IN_MINUTE = 60


def iso_time_to_timestamp(iso_time):
    d = dateutil.parser.parse(iso_time)
    d = d.replace(tzinfo=None)
    return int((d - datetime.datetime(1970, 1, 1)).total_seconds())


def round_down(timestamp, period):
    return int(timestamp - (timestamp % period))


def round_down_to_minute(timestamp):
    return round_down(timestamp, SECONDS_IN_MINUTE)


def filter_dict_keys(dict_, allowed_keys):
    result = deepcopy(dict_)
    for key in list(dict_):
        if key not in allowed_keys:
            del result[key]
    return result


class BoundProcess(Process):
    def __init__(self, group=None, target=None, name=None, terminate=None, args=None, kwargs=None,
                 pdeathsig=signal.SIGTERM):
        self._parent_pid = os.getpid()

        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}

        def kill():
            odin_logger.info("Killing process %d", self._parent_pid)
            try:
                os.kill(self._parent_pid, signal.SIGINT)
                odin_logger.info("Process %d killed by SIGINT", self._parent_pid)
            except OSError:
                odin_logger.info("Process %d is missing", self._parent_pid)
                return
            time.sleep(0.5)
            try:
                os.kill(self._parent_pid, signal.SIGTERM)
                odin_logger.info("Process %d killed by SIGTERM", self._parent_pid)
            except OSError:
                odin_logger.info("Process %d is missing", self._parent_pid)
                return
            time.sleep(0.5)
            try:
                os.kill(self._parent_pid, signal.SIGKILL)
                odin_logger.info("Process %d killed by SIGKILL", self._parent_pid)
            except OSError:
                odin_logger.info("Process %d is missing", self._parent_pid)
                return

        def safe_run(*args, **kwargs):
            try:
                import prctl
                prctl.set_pdeathsig(pdeathsig)
                if name:
                    prctl.set_name(name)
            except ImportError:
                pass

            try:
                target(*args, **kwargs)
            except:  # noqa
                odin_logger.exception("Process %d failed, killing parent process", os.getpid())
                kill()

        super(BoundProcess, self).__init__(group=group, target=safe_run, name=name, args=args, kwargs=kwargs)


def ceil_div(a, b):
    return (a + b - 1) // b


def get_part(keys, part_id, part_count):
    assert 0 <= part_id < part_count
    sorted_keys = sorted(keys)
    part_len = ceil_div(len(sorted_keys), part_count)
    offset = part_id * part_len
    return sorted_keys[offset: offset + part_len]
