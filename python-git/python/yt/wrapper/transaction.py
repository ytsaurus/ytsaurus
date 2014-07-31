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
    It is designed to use by with_statement::

    >>> with Transaction():
    >>>    ...
    >>>    lock("//home/my_node")
    >>>    ...
    >>>    with Transaction():
    >>>        ...
    >>>        yt.run_map(...)
    >>>

    Caution: if you use this class then do not use directly methods *_transaction.

    .. seealso:: `transactions on wiki <https://wiki.yandex-team.ru/yt/userdoc/transactions>`_
    """
    stack = []

    initial_transaction = "0-0-0-0"
    initial_ping_ancestor_transactions = False

    def __init__(self, timeout=None, attributes=None, null=False, client=None):
        self.client = get_value(client, config.CLIENT)
        self.null = null

        if self.client is None:
            if not Transaction.stack:
                Transaction.initial_transaction = config.TRANSACTION
                Transaction.initial_ping_ancestor_transactions = config.PING_ANCESTOR_TRANSACTIONS

        if self.null:
            self.transaction_id = "0-0-0-0"
        else:
            self.transaction_id = start_transaction(timeout=timeout, attributes=attributes, client=client)
        if self.client is None:
            Transaction.stack.append(self.transaction_id)
            self._update_global_config()
        else:
            # TODO(ignat): eliminate hack with ping ancestor transactions
            self.client._add_transaction(self.transaction_id, True)

        self.finished = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.finished:
            return
        try:
            if not self.null:
                if type is not None:
                    logger.warning(
                        "Error: (type=%s, value=%s, traceback=%s), aborting transaction %s ...",
                        type,
                        value,
                        tb.format_exc(traceback).replace("\n", "\\n"),
                        self.transaction_id)

                try:
                    if type is None:
                        commit_transaction(self.transaction_id, client=self.client)
                    else:
                        abort_transaction(self.transaction_id, client=self.client)
                except YtResponseError as rsp:
                    if rsp.is_resolve_error():
                        logger.warning("Transaction %s is absent, cannot commit or abort" % self.transaction_id)
                    else:
                        raise
        finally:
            if self.client is None:
                Transaction.stack.pop()
                self.finished = True
                self._update_global_config()
            else:
                self.client._pop_transaction()


    def _update_global_config(self):
        if Transaction.stack:
            config.TRANSACTION = Transaction.stack[-1]
            config.PING_ANCESTOR_TRANSACTIONS = True
        else:
            config.TRANSACTION = Transaction.initial_transaction
            config.PING_ANCESTOR_TRANSACTIONS = Transaction.initial_ping_ancestor_transactions

class PingTransaction(Thread):
    """
    Pinger for transaction.

    Ping transaction in background thread.
    """
    def __init__(self, transaction, delay, client=None):
        """
        :param delay: delay in seconds
        """
        super(PingTransaction, self).__init__()
        self.transaction = transaction
        self.delay = delay
        self.is_running = True
        self.step = config.TRANSACTION_SLEEP_PERIOD / 1000.0 # in seconds
        self.client = client

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def stop(self):
        self.is_running = False
        timeout = config.http.REQUEST_TIMEOUT / 1000.0
        # timeout should be enough to execute ping
        self.join(timeout + 2 * self.step)
        if self.is_alive():
            logger.warning("Ping request could not be completed within %.1lf seconds", timeout)

    def run(self):
        while self.is_running:
            ping_transaction(self.transaction, client=self.client)
            start_time = datetime.now()
            while datetime.now() - start_time < timedelta(seconds=self.delay):
                sleep(self.step)
                if not self.is_running:
                    return


class PingableTransaction(object):
    """Self-pinged transaction"""
    def __init__(self, timeout=None, attributes=None, client=None):
        self.timeout = get_value(timeout, config.http.REQUEST_TIMEOUT)
        self.attributes = attributes
        self.client = client

    def __enter__(self):
        self.transaction = Transaction(self.timeout, self.attributes, client=self.client)
        self.transaction.__enter__()

        transaction = config.TRANSACTION if self.client is None else self.client._get_transaction()[0]
        self.ping = PingTransaction(transaction, delay=self.timeout / (1000 * 10), client=self.client)
        self.ping.start()
        return self

    def __exit__(self, type, value, traceback):
        self.ping.stop()
        self.transaction.__exit__(type, value, traceback)

