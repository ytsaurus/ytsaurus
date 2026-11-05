import copy
from collections import Counter

from yt.wrapper import YtError

from .inserter import Inserter
from .runner import ThreadWrapper, Periodic, ThreadStatistics
from .log import logger
from .common import default_client_factory


class Selecter(ThreadWrapper, Periodic):
    def __init__(self, inserter: Inserter, period=0,
                 table_path=None, replica_consistency="sync",
                 client_factory=default_client_factory):
        self.inserter = inserter
        self.table_path = table_path if table_path is not None else inserter.table_path
        self.replica_consistency = replica_consistency
        self.client_factory = client_factory
        self.request_count = 0
        self.failed_count = 0
        self.fixed_uncertain_count = 0
        super().__init__(period=period)

    def select_and_check(self, read_table=False, trace=None):
        with self.inserter.lock:
            keys = self.inserter.keys[-500:]
        if keys:
            first_key = keys[0]
        else:
            first_key = 0

        client = self.client_factory(trace=trace)

        logger.debug("Starting select")

        with self.inserter.lock:
            old_correct = {key: self.inserter.rows[key] for key in keys if key in self.inserter.rows}

        try:
            if read_table:
                actual = list(client.read_table(self.table_path))
            else:
                if self.replica_consistency:
                    client.COMMAND_PARAMS.update({"replica_consistency": self.replica_consistency})
                actual = list(client.select_rows(
                    f'* from [{self.table_path}] where key < 1000000 and key >= {first_key} and "{trace}" = "{trace}" order by key limit 1000000'))
            self.request_count += 1
        except YtError as e:
            logger.info(f"Select failed: {e.error['message']}")
            if e.contains_text("Cannot read from tablet") and e.contains_text("while it is in \"unmounted\" state") or \
                    e.contains_text("Chunk data is not preloaded yet") or \
                    e.contains_text("Cell ") and e.contains_text(" is not active") or \
                    e.contains_text("Cell ") and e.contains_text(" is not known") or \
                    e.contains_text("Not an active leader"):
                raise
            logger.debug("Select failed", exc_info=True)
            self.failed_count += 1
            raise
        #  except TimeoutError:
        #      logger.info(f"Select failed: timed out")
        #      self.failed_count += 1
        #      continue

        actual_keys = [row["key"] for row in actual]
        if len(actual_keys) != len(set(actual_keys)):
            counts = Counter(actual_keys)
            raise Exception(f"Double write detected: {[item for item, count in counts.items() if count > 1]}")

        actual = {row["key"]: row for row in actual}
        for row in actual.values():
            row.pop("$row_index", None)
            row.pop("$tablet_index", None)

        #  logger.info(f"Actual: {actual}")

        logger.debug(f"Selected {len(actual)} rows")

        def _are_rows_equal(lhs, rhs):
            if (lhs is None) != (rhs is None):
                return False
            lhs = copy.deepcopy(lhs)
            lhs.pop("hash", None)
            rhs = copy.deepcopy(rhs)
            rhs.pop("hash", None)
            return lhs == rhs

        with self.inserter.lock:
            logger.debug(f"Uncertain: {self.inserter.uncertain_keys}")
            #  logger.info(f"Pending: {self.inserter.pending_rows}")
            for key in keys:
                if key not in actual:
                    if key in old_correct:
                        if self.replica_consistency is None:
                            logger.debug(f"Missing key {key} in async replica, skipping")
                            continue
                        raise Exception(f"Missing key {key}")
                    else:
                        if key in self.inserter.uncertain_keys:
                            logger.debug(f"Didn't get uncertain key {key}, will drop")
                            self.fixed_uncertain_count += 1
                            self.inserter.uncertain_keys.remove(key)
                        continue
                if _are_rows_equal(
                        self.inserter.rows.get(key, None), actual[key]) or \
                        _are_rows_equal(self.inserter.pending_rows.get(key, None), actual[key]) or \
                        _are_rows_equal(old_correct.get(key, None), actual[key]):
                    if key in self.inserter.uncertain_keys:
                        logger.debug(f"Key {key} removed from uncertain list")
                        self.fixed_uncertain_count += 1
                        self.inserter.uncertain_keys.remove(key)
                    continue
                if key in self.inserter.uncertain_keys:
                    logger.debug(f"Got uncertain key {key}, will update")
                    self.inserter.rows[key] = actual[key]
                    self.inserter.uncertain_keys.remove(key)
                    self.fixed_uncertain_count += 1
                    continue

                logger.error(f"Actual value for key {key}: {actual[key]}")
                logger.error(f"Stored value for key {key}: {self.inserter.rows.get(key)}")
                logger.error(f"Pending value for key {key}: {self.inserter.pending_rows.get(key)}")
                raise Exception("Invalid value for key {key}")

        logger.debug("Selected OK")

    def do_run_once(self, trace=None):
        self.select_and_check(trace=trace)

    def get_sleep_error_message(self, delay):
        return f"Waiting for {delay}s until next select"

    def do_collect_statistics(self):
        custom_message = (
            f"Selected {self.request_count} times, "
            f"failed {self.failed_count} times, "
            f"fixed {self.fixed_uncertain_count} uncertain keys")
        result = ThreadStatistics(self.request_count, self.failed_count, custom_message, "Select from " + self.table_path)
        self.request_count = 0
        self.failed_count = 0
        self.fixed_uncertain_count = 0
        return result
