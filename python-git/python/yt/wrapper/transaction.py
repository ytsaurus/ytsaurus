import config
import yt.logger as logger
from common import get_value
from errors import YtResponseError
from transaction_commands import start_transaction, commit_transaction, abort_transaction, ping_transaction

from thread import interrupt_main
from time import sleep
from threading import Thread
from datetime import datetime, timedelta

class Abort(Exception):
    pass

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

    def __init__(self, timeout=None, attributes=None, null=False, ping_ancestor_transactions=False, client=None):
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
            Transaction.stack.append((self.transaction_id, ping_ancestor_transactions))
            self._update_global_config()
        else:
            self.client._add_transaction(self.transaction_id, ping_ancestor_transactions)

        self.finished = False

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.finished:
            return
        try:
            if not self.null:
                if type is not None and type is not Abort:
                    logger.warning(
                        "Error: (type=%s, value=%s), aborting transaction %s ...",
                        type,
                        value,
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
            config.TRANSACTION, config.PING_ANCESTOR_TRANSACTIONS = Transaction.stack[-1]
        else:
            config.TRANSACTION, config.PING_ANCESTOR_TRANSACTIONS = \
                Transaction.initial_transaction, Transaction.initial_ping_ancestor_transactions

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
        self.daemon = True
        self.step = min(self.delay, config.TRANSACTION_SLEEP_PERIOD / 1000.0) # in seconds
        self.client = client

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def stop(self):
        if not self.is_running:
            return
        self.is_running = False
        timeout = config.http.get_timeout() / 1000.0
        # timeout should be enough to execute ping
        self.join(timeout + 2 * self.step)
        if self.is_alive():
            logger.warning("Ping request could not be completed within %.1lf seconds", timeout)

    def run(self):
        while self.is_running:
            try:
                ping_transaction(self.transaction, client=self.client)
            except:
                interrupt_main()
            start_time = datetime.now()
            while datetime.now() - start_time < timedelta(seconds=self.delay):
                sleep(self.step)
                if not self.is_running:
                    return


class PingableTransaction(object):
    """Self-pinged transaction"""
    def __init__(self, timeout=None, attributes=None, ping_ancestor_transactions=False, client=None):
        self.timeout = get_value(timeout, config.http.get_timeout())
        self.attributes = attributes
        self.ping_ancestor_transactions = ping_ancestor_transactions
        self.client = client

    def __enter__(self):
        self.transaction = Transaction(
            timeout=self.timeout,
            attributes=self.attributes,
            ping_ancestor_transactions=self.ping_ancestor_transactions,
            client=self.client).__enter__()

        transaction = config.TRANSACTION if self.client is None else self.client._get_transaction()[0]
        delay = (self.timeout / 1000.0) / max(2, config.http.REQUEST_RETRY_COUNT)
        self.ping = PingTransaction(transaction, delay, client=self.client)
        self.ping.start()
        return self

    def __exit__(self, type, value, traceback):
        self.ping.stop()
        self.transaction.__exit__(type, value, traceback)
