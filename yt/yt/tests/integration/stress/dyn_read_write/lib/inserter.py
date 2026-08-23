import threading

from yt.wrapper import YtError
from time import sleep

from .common import default_client_factory, random_string
from .log import logger
from .runner import ThreadWrapper, Periodic, ThreadStatistics


class Inserter(ThreadWrapper, Periodic):
    def __init__(self, table_path, period=0, client_factory=default_client_factory):
        self.table_path = table_path
        self.client_factory = client_factory

        self.rows = {}
        self.uncertain_keys = set()
        self.pending_rows = {}
        self.keys = []

        self.next_row = 0

        self.lock = threading.Lock()

        super().__init__(period=period)

        self.inserted_count = 0
        self.uncertain_count = 0

        self.valid_errors = [
            "has no mounted tablets",
            "Service is unable to complete your request",
            "No alive peers found",
            "Too many dynamic stores in tablet, all writes disabled",
            "Abort was requested for transaction",
            "transient_abort_prepared",
            "Not an active leader",
            "No such transaction",
            "was aborted",
        ]

    def insert(self, count, trace=None):
        client = self.client_factory(trace=trace)
        new_rows = [
            {
                "key": i,
                "value": "A" + str(i) + "____" + random_string(100) + "B"
            } for i in range(self.next_row, self.next_row + count)
        ]
        self.next_row += count

        with self.lock:
            for row in new_rows:
                key = row["key"]
                self.keys.append(key)
                self.pending_rows[key] = row

        row_range = f"{[row['key'] for row in new_rows]}"

        try:
            logger.debug(f"Inserting rows {row_range}")
            client.insert_rows(self.table_path, new_rows)
            with self.lock:
                for row in new_rows:
                    key = row["key"]
                    del self.pending_rows[key]
                    self.uncertain_keys.discard(key)
                    self.rows[key] = row
            logger.debug(f"Inserted OK {row_range}")
            self.inserted_count += len(new_rows)
        except YtError as e:
            err = str(e.simplify())
            if any(e.contains_text(text) for text in self.valid_errors):
                ignored_substrings = ", ".join(text for text in self.valid_errors if e.contains_text(str(text)))
                logger.info(f"Failed to insert rows (ignored: {ignored_substrings}) {row_range}: {e.error['message']}")
            elif "retry_limit_exceeded" in err:
                # logger.info(f"Failed to insert rows (retry_limit_exceeded) {row_range}: {e.error['message']}")
                logger.debug(f"Failed to insert rows (retry_limit_exceeded) {row_range}, better to investivage", exc_info=True)
            elif "Error sending transaction rows" not in err:
                logger.debug(f"Failed to insert rows (but not error_sending_transaction_rows) {row_range}", exc_info=True)
            elif e.contains_text("No such tablet"):
                # inner_error = e.find_matching_error(predicate=lambda error: "skip_retry_reason" in error.attributes)
                # print(inner_error.attributes)
                # print(str(inner_error))
                if "logical_mount_revision_changed" in err:
                    logger.info(f"Failed to insert rows (logical_mount_revision_changed) {row_range}: {e.error['message']}")
                else:
                    logger.debug(f"INTERESTING, Failed to insert rows {row_range}", exc_info=True)
            else:
                logger.debug(f"INTERESTING, Failed to insert rows {row_range}", exc_info=True)

            with self.lock:
                for row in new_rows:
                    key = row["key"]
                    del self.pending_rows[key]
                    self.uncertain_keys.add(key)

            self.uncertain_count += len(new_rows)
            raise

    def do_run_once(self, batch_size, trace=None):
        self.insert(batch_size, trace=trace)

    def do_collect_statistics(self):
        custom_message = f"Inserted {self.inserted_count} rows, uncertain {self.uncertain_count} rows"
        result = ThreadStatistics(
            self.inserted_count + self.uncertain_count,
            self.uncertain_count,
            custom_message,
            "Insert into " + self.table_path)
        self.inserted_count = 0
        self.uncertain_count = 0
        return result

    def get_sleep_error_message(self, delay):
        return f"Waiting for {delay}s until next insertion"

    def initialize(self):
        client = self.client_factory()

        def _try_initialize():
            try:
                result = list(client.select_rows(
                    f"* from [{self.table_path}] where key < 1000000 order by key limit 1000000"))
                global rows
                rows = result
                return True
            except YtError as e:
                err = str(e.simplify())
                if "No cluster contains in-sync replicas for table" in err:
                    logger.debug(
                        "Failed to initialize inserter due to "
                        "\"No cluster contains in-sync replicas for table\", retrying")
                    return False
                else:
                    raise

        while not _try_initialize():
            sleep(1)

        for row in rows:
            key = row["key"]
            self.keys.append(key)
            self.rows[key] = row

        if self.keys:
            self.next_row = max(self.keys) + 1
