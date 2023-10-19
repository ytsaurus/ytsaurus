from .common import YtError, total_seconds, get_arg_spec
from .errors import YtRetriableError, YtResponseError
from yt import logger as yt_logger

import abc
import copy
import random
import time
from datetime import datetime, timedelta


def run_with_retries(
    action, retry_count=6, backoff=20.0, exceptions=(YtError,), except_action=None,
    backoff_action=None, additional_retriable_error_codes=None
):
    if additional_retriable_error_codes is None:
        additional_retriable_error_codes = []

    class SimpleRetrier(Retrier):
        def __init__(self):
            retry_config = {
                "enable": True,
                "count": retry_count,
                "total_timeout": None,
                "backoff": {"policy": "rounded_up_to_request_timeout"},
                "additional_retriable_error_codes": additional_retriable_error_codes,
            }
            super(SimpleRetrier, self).__init__(retry_config, backoff * 1000.0, exceptions)
            self.exception = None

        def action(self):
            return action()

        def except_action(self, exception, attempt):
            if except_action:
                if len(get_arg_spec(except_action).args) == 0:
                    except_action()
                else:
                    except_action(exception)
            self.exception = exception

        def backoff_action(self, attempt, backoff):
            if backoff_action:
                backoff_action(self.exception, attempt, backoff)

            time.sleep(backoff)

    return SimpleRetrier().run()


def default_chaos_monkey(enable):
    def chaos_monkey():
        return enable and random.randint(1, 5) == 1
    return chaos_monkey


def run_chaos_monkey(chaos_monkey):
    if chaos_monkey is not None and chaos_monkey():
        raise YtRetriableError()


class Retrier(object):
    def __init__(
        self, retry_config, timeout=None, exceptions=(YtError,),
        ignore_exceptions=tuple(), chaos_monkey=None, logger=None
    ):
        self.retry_config = copy.deepcopy(retry_config)
        self.exceptions = exceptions
        self.ignore_exceptions = ignore_exceptions
        self.timeout = timeout
        self._chaos_monkey = chaos_monkey
        self._logger = logger if logger is not None else yt_logger

    def run(self):
        retry_count = self.retry_config.get("count")
        total_timeout = self.retry_config.get("total_timeout")
        if total_timeout is not None:
            if not isinstance(total_timeout, timedelta):
                total_timeout = timedelta(seconds=total_timeout / 1000.0)
            deadline = datetime.now() + total_timeout
        else:
            deadline = None

        attempt = 1
        while True:
            attempt_start_time = datetime.now()
            try:
                run_chaos_monkey(self._chaos_monkey)
                return self.action()
            except self.exceptions + (YtResponseError, ) as exception:
                if (retry_count is not None and attempt == retry_count) or not self.retry_config["enable"]:
                    raise

                is_error_retriable = False

                if isinstance(exception, self.exceptions) and not isinstance(exception, self.ignore_exceptions):
                    is_error_retriable = True
                else:
                    for retriable_code in self.retry_config.get("additional_retriable_error_codes", []):
                        if exception.contains_code(retriable_code):
                            is_error_retriable = True
                            break

                if not is_error_retriable:
                    raise

                self.except_action(exception, attempt)

                backoff = self.get_backoff(attempt, attempt_start_time)
                if deadline is not None and datetime.now() + timedelta(seconds=backoff) > deadline:
                    raise

                self.backoff_action(attempt, backoff)

                attempt += 1

    @abc.abstractmethod
    def action(self):
        pass

    def get_backoff(self, attempt, start_time):
        backoff_config = self.retry_config["backoff"]
        now = datetime.now()

        if backoff_config["policy"] == "rounded_up_to_request_timeout":
            return max(0.0, self.timeout / 1000.0 - total_seconds(now - start_time))
        elif backoff_config["policy"] == "constant_time":
            return backoff_config["constant_time"] / 1000.0
        elif backoff_config["policy"] == "exponential":
            exponential_policy = backoff_config["exponential_policy"]
            backoff = (exponential_policy["start_timeout"] / 1000.0) * (exponential_policy["base"] ** attempt)
            timeout = min(exponential_policy["max_timeout"] / 1000.0, backoff)
            return timeout * (1.0 + exponential_policy["decay_factor_bound"] * random.random())
        else:
            raise YtError("Incorrect retry backoff policy '{0}'".format(backoff_config["policy"]))

    def backoff_action(self, attempt, backoff):
        self._logger.warning("Sleep for %.2lf seconds before next retry", backoff)
        time.sleep(backoff)
        self._logger.warning("New retry (%d) ...", attempt + 1)

    def except_action(self, exception, attempt):
        pass


class IteratorRetrier(Retrier):
    def __init__(self, retry_config, timeout=None, exceptions=(YtError,), chaos_monkey=None):
        super(IteratorRetrier, self).__init__(retry_config, timeout, exceptions,
                                              ignore_exceptions=(), chaos_monkey=chaos_monkey)
        self._iter = None

    def action(self):
        if self._iter is None:
            self._iter = self._iterator()
        return next(self._iter)

    def except_action(self, exception, attempt):
        self._iter = None

    def run(self):
        while True:
            try:
                yield super(IteratorRetrier, self).run()
            except StopIteration:
                return

    @abc.abstractmethod
    def _iterator(self):
        pass
