import config
import logger
from common import require, get_value
from errors import YtError
from tree_commands import exists
from transaction_commands import start_transaction, commit_transaction, abort_transaction, ping_transaction

import os
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
    initial_ping_ansector_transactions = False

    def __init__(self, timeout=None, attributes=None):
        if not Transaction.stack:
            Transaction.initial_transaction = config.TRANSACTION
            Transaction.initial_ping_ansector_transactions = config.PING_ANSECTOR_TRANSACTIONS

        self.transaction_id = start_transaction(timeout=timeout, attributes=attributes)
        Transaction.stack.append(self.transaction_id)

        self._update_global_config()

        self.finished = False

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        if self.finished:
            return
        try:
            if type is not None:
                logger.warning(
                    "Error: (type=%s, value=%s, traceback=%s), aborting transaction %s ...",
                    type,
                    value,
                    traceback,
                    self.transaction_id)
            if not exists("#" + self.transaction_id):
                logger.warning("Transaction %s is absent, cannot commit or abort" % self.transaction_id)
            elif type is None:
                commit_transaction(self.transaction_id)
            else:
                abort_transaction(self.transaction_id)
        finally:
            Transaction.stack.pop()
            self.finished = True
            self._update_global_config()

    def _update_global_config(self):
        if Transaction.stack:
            config.TRANSACTION = Transaction.stack[-1]
            config.PING_ANSECTOR_TRANSACTIONS = True
        else:
            config.TRANSACTION = Transaction.initial_transaction
            config.PING_ANSECTOR_TRANSACTIONS = Transaction.initial_ping_ansector_transactions

class PingTransaction(Thread):
    # delay and step in seconds
    def __init__(self, transaction, delay):
        super(PingTransaction, self).__init__()
        self.transaction = transaction
        self.delay = delay
        self.is_running = True
        self.step = 0.1

    def __enter__(self):
        self.start()

    def __exit__(self, type, value, traceback):
        self.stop()

    def stop(self):
        self.is_running = False
        # 5.0 seconds correction for waiting response from ping
        self.join(5.0 + self.step)
        require(not self.is_alive(), YtError("Ping request could not be completed within 5 seconds"))

    def run(self):
        try:
            while self.is_running:
                ping_transaction(self.transaction)
                start_time = datetime.now()
                while datetime.now() - start_time < timedelta(seconds=self.delay):
                    sleep(self.step)
                    if not self.is_running:
                        return
        except KeyboardInterrupt:
            self.interrupt_main()


class PingableTransaction(object):
    def __init__(self, timeout=None, attributes=None):
        self.timeout = get_value(timeout, config.TRANSACTION_TIMEOUT)
        self.attributes = attributes

    def __enter__(self):
        self.transaction = Transaction(self.timeout, self.attributes)
        self.transaction.__enter__()

        self.ping = PingTransaction(config.TRANSACTION, delay=self.timeout / (1000 * 10))
        self.ping.start()

    def __exit__(self, type, value, traceback):
        self.ping.stop()
        self.transaction.__exit__(type, value, traceback)

