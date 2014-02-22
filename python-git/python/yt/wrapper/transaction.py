import config
import yt.logger as logger
from common import get_value
from errors import YtResponseError
from transaction_commands import start_transaction, commit_transaction, abort_transaction, ping_transaction

import traceback as tb
from time import sleep
from threading import Thread
from datetime import datetime, timedelta

class Transaction(object):
    """
    It is designed to use by with_statement:
    > with Transaction():
    >    ....
    >    lock("//home/my_node")
    >    ....
    >    with Transaction():
    >        ....
    >        yt.run_map(...)
    >
    Caution: if you use this class then do not use directly methods *_transaction.
    """
    stack = []

    initial_transaction = "0-0-0-0"
    initial_ping_ancestor_transactions = False

    def __init__(self, timeout=None, attributes=None):
        if not Transaction.stack:
            Transaction.initial_transaction = config.TRANSACTION
            Transaction.initial_ping_ancestor_transactions = config.PING_ANCESTOR_TRANSACTIONS

        self.transaction_id = start_transaction(timeout=timeout, attributes=attributes)
        Transaction.stack.append(self.transaction_id)

        self._update_global_config()

        self.finished = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.finished:
            return
        try:
            if type is not None:
                logger.warning(
                    "Error: (type=%s, value=%s, traceback=%s), aborting transaction %s ...",
                    type,
                    value,
                    tb.format_exc(traceback).replace("\n", "\\n"),
                    self.transaction_id)

            try:
                if type is None:
                    commit_transaction(self.transaction_id)
                else:
                    abort_transaction(self.transaction_id)
            except YtResponseError as rsp:
                if rsp.is_resolve_error():
                    logger.warning("Transaction %s is absent, cannot commit or abort" % self.transaction_id)
                else:
                    raise
        finally:
            Transaction.stack.pop()
            self.finished = True
            self._update_global_config()

    def _update_global_config(self):
        if Transaction.stack:
            config.TRANSACTION = Transaction.stack[-1]
            config.PING_ANCESTOR_TRANSACTIONS = True
        else:
            config.TRANSACTION = Transaction.initial_transaction
            config.PING_ANCESTOR_TRANSACTIONS = Transaction.initial_ping_ancestor_transactions

class PingTransaction(Thread):
    # delay and step in seconds
    def __init__(self, transaction, delay):
        super(PingTransaction, self).__init__()
        self.transaction = transaction
        self.delay = delay
        self.is_running = True
        self.step = config.TRANSACTION_PING_BACKOFF / 1000.0

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def stop(self):
        self.is_running = False
        timeout = config.http.REQUEST_TIMEOUT
        # timeout should be enough to execute ping
        self.join(timeout + 2 * self.step)
        if self.is_alive():
            logger.warning("Ping request could not be completed within %.1lf seconds", timeout)

    def run(self):
        while self.is_running:
            ping_transaction(self.transaction)
            start_time = datetime.now()
            while datetime.now() - start_time < timedelta(seconds=self.delay):
                sleep(self.step)
                if not self.is_running:
                    return


class PingableTransaction(object):
    def __init__(self, timeout=None, attributes=None):
        self.timeout = get_value(timeout, config.TRANSACTION_TIMEOUT)
        self.attributes = attributes

    def __enter__(self):
        self.transaction = Transaction(self.timeout, self.attributes)
        self.transaction.__enter__()

        self.ping = PingTransaction(config.TRANSACTION, delay=self.timeout / (1000 * 10))
        self.ping.start()
        return self

    def __exit__(self, type, value, traceback):
        self.ping.stop()
        self.transaction.__exit__(type, value, traceback)

