from config import get_option, set_option, get_config, get_total_request_timeout, get_request_retry_count
import yt.logger as logger
from common import get_value
from errors import YtResponseError
from transaction_commands import start_transaction, commit_transaction, abort_transaction, ping_transaction

from copy import deepcopy
from thread import interrupt_main
from time import sleep
from threading import Thread
from datetime import datetime, timedelta

class Abort(Exception):
    pass

class TransactionStack(object):
    def __init__(self):
        self.stack = []
        self.initial_transaction = "0-0-0-0"
        self.initial_ping_ancestor_transactions = False

    def init(self, initial_transaction, initial_ping_ancestor_transactions):
        if not self.stack:
            self.initial_transaction = initial_transaction
            self.initial_ping_ancestor_transactions = initial_ping_ancestor_transactions

    def append(self, transaction_id, ping_ancestor_transactions):
        self.stack.append((transaction_id, ping_ancestor_transactions))

    def pop(self):
        self.stack.pop()

    def get(self):
        if self.stack:
            return self.stack[-1]
        else:
            return self.initial_transaction, self.initial_ping_ancestor_transactions


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

    def __init__(self, timeout=None, attributes=None, ping=True, null=False,
                 ping_ancestor_transactions=False, client=None):
        timeout = get_value(timeout, get_total_request_timeout(client))

        self.null = null
        self.client = client
        self.ping = ping
        self.timeout = timeout
        self.ping_ancestor_transactions = ping_ancestor_transactions
        self.attributes = deepcopy(attributes)

        if get_option("_transaction_stack", self.client) is None:
            set_option("_transaction_stack", TransactionStack(), self.client)
        self.stack = get_option("_transaction_stack", self.client)

        self.stack.init(get_option("TRANSACTION", self.client), get_option("PING_ANCESTOR_TRANSACTIONS", self.client))

        self.finished = False

    def __enter__(self):
        if not self.finished:
            self._start_new_transaction()
            if self.ping and not self.null:
                delay = (self.timeout / 1000.0) / max(2, get_request_retry_count(self.client))
                self.ping_thread = PingTransaction(self.transaction_id, delay, client=self.client)
                self.ping_thread.start()
        return self

    def __exit__(self, type, value, traceback):
        if self.finished:
            return
        if self.ping and not self.null:
            self.ping_thread.stop()
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
                        logger.warning("Transaction %s is missing, cannot commit or abort" % self.transaction_id)
                    else:
                        raise
        finally:
            self.stack.pop()
            transaction_id, ping_ancestor_transactions = self.stack.get()
            set_option("TRANSACTION", transaction_id, self.client)
            set_option("PING_ANCESTOR_TRANSACTIONS", ping_ancestor_transactions, self.client)
            self.finished = True

    def _start_new_transaction(self):
        if self.null:
            self.transaction_id = "0-0-0-0"
        else:
            self.transaction_id = start_transaction(timeout=self.timeout,
                                                    attributes=self.attributes,
                                                    client=self.client)

        self.stack.append(self.transaction_id, self.ping_ancestor_transactions)
        set_option("TRANSACTION", self.transaction_id, self.client)
        set_option("PING_ANCESTOR_TRANSACTIONS", self.ping_ancestor_transactions, self.client)

class EmptyTransaction(object):
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def __nonzero__(self):
        return False

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
        self.step = min(self.delay, get_config(client)["transaction_sleep_period"] / 1000.0) # in seconds
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
        timeout = get_total_request_timeout(self.client) / 1000.0
        # timeout should be enough to execute ping
        self.join(timeout + 2 * self.step)
        if self.is_alive():
            logger.warning("Ping request could not be completed within %.1lf seconds", timeout)

    def run(self):
        while self.is_running:
            try:
                ping_transaction(self.transaction, client=self.client)
            except:
                logger.exception("Ping failed")
                interrupt_main()
            start_time = datetime.now()
            while datetime.now() - start_time < timedelta(seconds=self.delay):
                sleep(self.step)
                if not self.is_running:
                    return

class PingableTransaction(Transaction):
    """Self-pinged transaction"""
    """Deprecated! Use Transaction(...ping=True...) instead"""
    def __init__(self, timeout=None, attributes=None, ping_ancestor_transactions=False, client=None):
        super(PingableTransaction, self).__init__(
            timeout=timeout,
            attributes=attributes,
            ping=True,
            null=False,
            ping_ancestor_transactions=ping_ancestor_transactions,
            client=client)

