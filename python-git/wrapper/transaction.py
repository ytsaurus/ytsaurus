from .config import (get_option, set_option, get_config, get_total_request_timeout,
                     get_request_retry_count, get_command_param, set_command_param, del_command_param)
from .common import get_value
from .errors import YtResponseError, YtError, YtTransactionPingError
from .transaction_commands import start_transaction, commit_transaction, abort_transaction, ping_transaction

from yt.common import YT_NULL_TRANSACTION_ID as null_transaction_id

import yt.logger as logger

from yt.packages.six.moves._thread import interrupt_main

from copy import deepcopy
from time import sleep
from threading import Thread
from datetime import datetime, timedelta
import signal
import os

_sigusr_received = False

def _get_ping_failed_mode(client):
    if get_config(client)["ping_failed_mode"] is not None:
        return get_config(client)["ping_failed_mode"]
    if get_config(client)["transaction_use_signal_if_ping_failed"]:
        return "send_signal"
    else:
        return "interrupt_main"

def _set_sigusr_received(value):
    global _sigusr_received
    _sigusr_received = value

def _sigusr_handler(signum, frame):
    # XXX(asaitgalin): handler is always executed in main thread so it is safe
    # to raise error here. This error will be caught by __exit__ method so it is ok.
    if signum == signal.SIGUSR1 and not _sigusr_received:
        _set_sigusr_received(True)
        raise YtTransactionPingError()

class TransactionStack(object):
    def __init__(self):
        self._stack = []
        self.initial_transaction = null_transaction_id
        self.initial_ping_ancestor_transactions = False

    def init(self, initial_transaction, initial_ping_ancestor_transactions):
        if not self._stack:
            self.initial_transaction = initial_transaction
            self.initial_ping_ancestor_transactions = initial_ping_ancestor_transactions

    def append(self, transaction_id, ping_ancestor_transactions):
        self._stack.append((transaction_id, ping_ancestor_transactions))

    def pop(self):
        self._stack.pop()

    def get(self):
        if self._stack:
            return self._stack[-1]
        else:
            return self.initial_transaction, self.initial_ping_ancestor_transactions


class Transaction(object):
    """
    It is designed to be used by with_statement::

      with Transaction():
         ...
         lock("//home/my_node")
         ...
         with Transaction():
             ...
             yt.run_map(...)

    Caution: if you use this class then do not use directly methods \*_transaction.

    .. seealso:: `transactions in the docs <https://yt.yandex-team.ru/docs/description/storage/transactions.html>`_
    """

    def __init__(self, timeout=None, deadline=None, attributes=None, ping=None, interrupt_on_failed=True, transaction_id=None,
                 ping_ancestor_transactions=None, type="master",
                 client=None):
        timeout = get_value(timeout, get_total_request_timeout(client))
        if transaction_id == null_transaction_id:
            ping = False

        self.transaction_id = transaction_id
        self.sticky = True if type == "tablet" else False
        self._client = client
        self._ping_ancestor_transactions = \
            get_value(ping_ancestor_transactions, get_command_param("ping_ancestor_transactions", self._client))
        self._ping = ping
        self._finished = False
        self._used_with_statement = False

        if get_option("_transaction_stack", self._client) is None:
            set_option("_transaction_stack", TransactionStack(), self._client)
        self._stack = get_option("_transaction_stack", self._client)
        self._stack.init(get_command_param("transaction_id", self._client),
                         get_command_param("ping_ancestor_transactions", self._client))
        if self.transaction_id is None:
            self.transaction_id = start_transaction(timeout=timeout,
                                                    deadline=deadline,
                                                    attributes=attributes,
                                                    type=type,
                                                    sticky=self.sticky,
                                                    client=self._client)
            self._started = True
            if self._ping is None:
                self._ping = True
        else:
            self._started = False

        if _get_ping_failed_mode(self._client) == "send_signal":
            _set_sigusr_received(False)
            self._old_sigusr_handler = signal.signal(signal.SIGUSR1, _sigusr_handler)

        if self._ping:
            # TODO(ignat): remove this local import
            from .client import YtClient
            pinger_client = YtClient(config=deepcopy(get_config(self._client)))
            # For sticky transaction we must use the same client as at transaction creation.
            for option in ("_driver", "_requests_session"):
                set_option(option, get_option(option, client=self._client), client=pinger_client)
            delay = (timeout / 1000.0) / max(2, get_request_retry_count(self._client))
            self._ping_thread = PingTransaction(
                self.transaction_id,
                delay,
                interrupt_on_failed=interrupt_on_failed,
                client=pinger_client)
            self._ping_thread.start()

    def abort(self):
        """Aborts transaction.

        NOTE: abort() must not be called explicitly when transaction is used with with_statement::

          with Transaction() as t:
              ...
              t.abort() # Wrong!
        """
        if self._used_with_statement:
            raise RuntimeError("Transaction is used with with_statement; explicit abort() is not allowed")
        if self._finished or self.transaction_id == null_transaction_id:
            return
        self._stop_pinger()
        abort_transaction(self.transaction_id, client=self._client)
        self._finished = True

    def commit(self):
        """Commits transaction.

        NOTE: commit() must not be called explicitly when transaction is used with with_statement::

          with Transaction() as t:
              ...
              t.commit() # Wrong!
        """
        if self._used_with_statement:
            raise RuntimeError("Transaction is used with with_statement; explicit commit() is not allowed")
        if self.transaction_id == null_transaction_id:
            return
        if self._finished:
            raise YtError("Transaction is already finished, cannot commit")
        self._stop_pinger()
        commit_transaction(self.transaction_id, client=self._client)
        self._finished = True

    def is_pinger_alive(self):
        """Checks if pinger is alive."""
        if self._ping:
            return not self._ping_thread.failed
        return True

    def __enter__(self):
        self._stack.append(self.transaction_id, self._ping_ancestor_transactions)
        enable_params_logging = get_config(self._client)["enable_logging_for_params_changes"]
        if enable_params_logging:
            logger.debug("Setting \"transaction_id\" and \"ping_ancestor_transactions\" to params (pid: %d)", os.getpid())
        if self.transaction_id is None:
            del_command_param("transaction_id", self._client)
        else:
            set_command_param("transaction_id", self.transaction_id, self._client)
        if self._ping_ancestor_transactions is None:
            del_command_param("ping_ancestor_transactions", self._client)
        else:
            set_command_param("ping_ancestor_transactions", self._ping_ancestor_transactions, self._client)
        if enable_params_logging:
            logger.debug("Set finished (pid: %d)", os.getpid())
        self._used_with_statement = True
        return self

    def __exit__(self, type, value, traceback):
        if self._finished:
            return

        # Allow abort() and commit() temorary
        self._used_with_statement = False
        try:
            if not self._started or self.transaction_id == null_transaction_id:
                return

            action = "commit" if type is None else "abort"
            if action == "abort":
                # NB: logger may be socket logger. In this case we should convert error to string to avoid
                # bug with unpickling Exception that has non-trivial __init__.
                logger.debug(
                    "Error %s, aborting transaction %s ...",
                    repr(value),
                    self.transaction_id)

            try:
                if action == "commit":
                    self.commit()
                else:
                    self.abort()
            except YtResponseError as rsp:
                if rsp.is_resolve_error():
                    logger.warning("Transaction %s is missing, cannot %s", self.transaction_id, action)
                    if action == "commit":
                        raise
                else:
                    raise
        finally:
            self._used_with_statement = True

            self._stack.pop()
            if _get_ping_failed_mode(self._client) == "send_signal":
                signal.signal(signal.SIGUSR1, self._old_sigusr_handler)
            transaction_id, ping_ancestor_transactions = self._stack.get()
            enable_params_logging = get_config(self._client)["enable_logging_for_params_changes"]
            if enable_params_logging:
                logger.debug("Setting \"transaction_id\" and \"ping_ancestor_transactions\" to params (pid: %d)", os.getpid())
            if transaction_id is None:
                del_command_param("transaction_id", self._client)
            else:
                set_command_param("transaction_id", transaction_id, self._client)
            if ping_ancestor_transactions is None:
                del_command_param("ping_ancestor_transactions", self._client)
            else:
                set_command_param("ping_ancestor_transactions", ping_ancestor_transactions, self._client)
            if enable_params_logging:
                logger.debug("Set finished (pid: %d)", os.getpid())

    def _stop_pinger(self):
        if self._ping:
            self._ping_thread.stop()


class PingTransaction(Thread):
    """Pinger for transaction.

    Pings transaction in background thread.
    """
    def __init__(self, transaction, delay, interrupt_on_failed=True, client=None):
        """
        :param int delay: delay in seconds.
        """
        super(PingTransaction, self).__init__()
        self.transaction = transaction
        self.delay = delay
        self.interrupt_on_failed = interrupt_on_failed
        self.failed = False
        self.is_running = True
        self.daemon = True
        self.step = min(self.delay, get_config(client)["transaction_sleep_period"] / 1000.0)  # in seconds
        self._client = client

        ping_failed_mode = _get_ping_failed_mode(self._client)
        if ping_failed_mode not in ("interrupt_main", "send_signal", "pass"):
            raise YtError("Incorrect ping failed mode {}, expects one of "
                          "(\"interrupt_main\", \"send_signal\", \"pass\")".format(ping_failed_mode))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def stop(self):
        if not self.is_running:
            return
        self.is_running = False
        timeout = get_total_request_timeout(self._client) / 1000.0
        # timeout should be enough to execute ping
        self.join(timeout + 2 * self.step)
        if self.is_alive():
            logger.warning("Ping request could not be completed within %.1lf seconds", timeout)

    def run(self):
        while self.is_running:
            try:
                ping_transaction(self.transaction, client=self._client)
            except:
                self.failed = True
                if self.interrupt_on_failed:
                    logger.exception("Ping failed")
                    ping_failed_mode = _get_ping_failed_mode(self._client)
                    if ping_failed_mode == "send_signal":
                        os.kill(os.getpid(), signal.SIGUSR1)
                    elif ping_failed_mode == "interrupt_main":
                        interrupt_main()
                    else:  # ping_failed_mode == "pass":
                        pass
                else:
                    logger.exception("Failed to ping transaction %s, pinger stopped", self.transaction)
                    return
            start_time = datetime.now()
            while datetime.now() - start_time < timedelta(seconds=self.delay):
                sleep(self.step)
                if not self.is_running:
                    return

    def __del__(self):
        self.stop()

def get_current_transaction_id(client=None):
    """
    Returns current transaction id of client.
    """
    transaction_stack = get_option("_transaction_stack", client)
    if not transaction_stack:
        return null_transaction_id
    else:
        return transaction_stack.get()[0]
