from .config import (get_option, set_option, get_config,
                     get_command_param, set_command_param, del_command_param, get_backend_type)
from .common import get_value, time_option_to_milliseconds
from .default_config import retries_config
from .errors import YtResponseError, YtError, YtTransactionPingError
from .transaction_commands import start_transaction, commit_transaction, abort_transaction, ping_transaction

from yt.common import YT_NULL_TRANSACTION_ID as null_transaction_id

import yt.logger as logger

from _thread import interrupt_main

from copy import deepcopy
from time import sleep
from threading import Thread, RLock
from datetime import datetime, timedelta
import signal
import time
import os
import logging
import sys

_sigusr_received = False

PING_FAILED_MODES = (
    "call_function",
    "interrupt_main",
    "pass",
    "send_signal",
    "terminate_process",
)


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

    Caution: if you use this class then do not use directly methods \\*_transaction.

    :param bool acquire: commit/abort transaction in exit from with. By default False if new transaction is not started else True and false values are not allowed.
    :param bool ping: ping transaction in separate thread. By default True if acquire is also True else False.

    .. seealso:: `transactions in the docs <https://ytsaurus.tech/docs/en/user-guide/storage/transactions>`_
    """

    def __init__(self, timeout=None, deadline=None, attributes=None, ping=None, interrupt_on_failed=True,
                 transaction_id=None, ping_ancestor_transactions=None, type="master", acquire=None,
                 ping_period=None, ping_timeout=None, prerequisite_transaction_ids=None,
                 client=None):
        if transaction_id == null_transaction_id:
            ping = False
            acquire = False

        self.transaction_id = transaction_id
        self.sticky = (type == "tablet" and get_backend_type(client) == "http")
        self._client = client
        self._ping_ancestor_transactions = \
            get_value(ping_ancestor_transactions, get_command_param("ping_ancestor_transactions", self._client))
        self._ping = ping
        self._acquire = acquire
        self._finished = False
        self._used_with_statement = False

        if get_option("_transaction_stack", self._client) is None:
            set_option("_transaction_stack", TransactionStack(), self._client)
        self._stack = get_option("_transaction_stack", self._client)
        self._stack.init(get_command_param("transaction_id", self._client),
                         get_command_param("ping_ancestor_transactions", self._client))

        if self.transaction_id is not None and prerequisite_transaction_ids:
            raise RuntimeError("prerequisite_transaction_ids={!r} must be None or empty when transaction_id is not None".format(prerequisite_transaction_ids))

        if self.transaction_id is None:
            if self._acquire is None:
                self._acquire = True
            elif not self._acquire:
                raise RuntimeError(
                    'acquire={!r} (false value) is not allowed when '
                    'transaction_id is not specified'
                    .format(self._acquire)
                )

            timeout = time_option_to_milliseconds(get_value(
                timeout,
                max(
                    # Some legacy logic.
                    max(
                        time_option_to_milliseconds(get_config(client)["proxy"]["retries"]["total_timeout"]),
                        time_option_to_milliseconds(get_config(client)["proxy"]["request_timeout"]),
                    ),
                    time_option_to_milliseconds(get_config(client)["transaction_timeout"]))
            ))
            self.transaction_id = start_transaction(timeout=timeout,
                                                    deadline=deadline,
                                                    attributes=attributes,
                                                    type=type,
                                                    sticky=self.sticky,
                                                    prerequisite_transaction_ids=prerequisite_transaction_ids,
                                                    client=self._client)
        elif self._ping:
            from .cypress_commands import get
            timeout = get("#{}/@timeout".format(self.transaction_id), client=client)

        if self._acquire is None:
            self._acquire = False

        if self._ping is None:
            self._ping = self._acquire

        if _get_ping_failed_mode(self._client) == "send_signal":
            _set_sigusr_received(False)
            self._old_sigusr_handler = signal.signal(signal.SIGUSR1, _sigusr_handler)

        if self._ping:
            # TODO(ignat): remove this local import
            from .client import YtClient
            pinger_client = YtClient(config=deepcopy(get_config(self._client)))
            # For sticky transaction we must use the same client as at transaction creation.
            for option in ("_driver", "_requests_session", "_token", "_token_cached",
                           "_api_version", "_commands", "_fqdn"):
                set_option(option, get_option(option, client=self._client), client=pinger_client)
            self._ping_thread = PingTransaction(
                self.transaction_id,
                # NB: ping_period + ping_timeout should be separated from total transaction timeout.
                ping_period=get_value(ping_period, timeout / 3),
                ping_timeout=get_value(ping_timeout, timeout / 3),
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
        try:
            abort_transaction(self.transaction_id, client=self._client)
        finally:
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

        self._prepare_stop_pinger()
        try:
            commit_transaction(self.transaction_id, client=self._client)
        finally:
            self._stop_pinger()
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
            logger.debug(
                "Setting \"transaction_id\" and \"ping_ancestor_transactions\" to params (pid: %d)",
                os.getpid())
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

        # Allow abort() and commit() temporary
        self._used_with_statement = False
        try:
            if not self._acquire:
                self._prepare_stop_pinger()
                self._stop_pinger()
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
                logger.debug(
                    "Setting \"transaction_id\" and \"ping_ancestor_transactions\" to params (pid: %d)",
                    os.getpid())
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

    def _prepare_stop_pinger(self):
        if self._ping:
            self._ping_thread.prepare_stop()


class PingTransaction(Thread):
    """Pinger for transaction.

    Pings transaction in background thread.
    """
    def __init__(self, transaction, ping_period, ping_timeout, interrupt_on_failed=True, client=None):
        """
        :param int ping_period: ping period in milliseconds.
        :param int ping_timeout: ping request timeout in milliseconds.
        """
        super(PingTransaction, self).__init__(name="PingTransaction")
        self.transaction = transaction
        self.ping_period = time_option_to_milliseconds(ping_period)
        self.ping_timeout = time_option_to_milliseconds(ping_timeout)
        self.interrupt_on_failed = interrupt_on_failed
        self.failed = False
        self.is_running = True
        self.daemon = True
        self.sleep_period = min(self.ping_period / 5, get_config(client)["transaction_sleep_period"])
        self.retry_config = retries_config(
            enable=True,
            total_timeout=self.ping_timeout,
            backoff={
                "policy": "constant_time",
                "constant_time": self.ping_period / 5,
            })
        self._client = client

        self.ignore_no_such_transaction_error = False

        ping_failed_mode = _get_ping_failed_mode(self._client)
        if ping_failed_mode not in PING_FAILED_MODES:
            raise YtError("Incorrect ping failed mode {}, expects one of {!r}".format(ping_failed_mode, PING_FAILED_MODES))
        if ping_failed_mode == "call_function":
            ping_failed_function = get_config(client)["ping_failed_function"]
            if not callable(ping_failed_function):
                raise YtError("Incorrect or missing ping_failed_function {}, must be callable".format(ping_failed_function))

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.stop()

    def prepare_stop(self):
        self.ignore_no_such_transaction_error = True

    def stop(self):
        if not self.is_running:
            return
        self.is_running = False
        if not self.is_alive():
            # If the thread has never been started, attempting to join it causes RuntimeError.
            return
        # timeout should be enough to execute ping
        timeout = (self.ping_timeout + 2 * self.sleep_period) / 1000.0
        self.join(timeout)
        if self.is_alive():
            logger.warning("Ping request could not be completed within %.1lf seconds", self.ping_timeout)

    def _process_failed_ping(self):
        self.failed = True
        if self.interrupt_on_failed:
            ping_failed_mode = _get_ping_failed_mode(self._client)
            logger.exception("Ping failed (ping_failed_mode: %s, is_running: %s)", ping_failed_mode, self.is_running)
            if ping_failed_mode == "send_signal":
                os.kill(os.getpid(), signal.SIGUSR1)
            elif ping_failed_mode == "interrupt_main":
                Thread(target=interrupt_main, name="ping_failed_interrupt_main").start()
            elif ping_failed_mode == "terminate_process":
                logging.shutdown()
                os.kill(os.getpid(), signal.SIGTERM)
            elif ping_failed_mode == "call_function":
                get_config(self._client)["ping_failed_function"]()
            else:  # ping_failed_mode == "pass":
                pass
        else:
            logger.exception("Failed to ping transaction %s, pinger stopped", self.transaction)

        self.is_running = False

    def run(self):
        while self.is_running:
            try:
                ping_transaction(self.transaction, timeout=self.ping_timeout, client=self._client)
            except YtError as err:
                if not self.ignore_no_such_transaction_error or not err.is_no_such_transaction():
                    self._process_failed_ping()
            except:  # noqa
                self._process_failed_ping()

            start_time = datetime.now()
            while datetime.now() - start_time < timedelta(seconds=self.ping_period / 1000.0):
                sleep(self.sleep_period / 1000.0)
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


class _TransactionAborter(Thread):
    def __init__(self):
        super(_TransactionAborter, self).__init__()
        self._lock = RLock()
        self._transactions_to_abort = []

    def add(self, transaction):
        with self._lock:
            self._transactions_to_abort.append(transaction)

    def run(self):
        while True:
            transaction = None
            with self._lock:
                if self._transactions_to_abort:
                    transaction = self._transactions_to_abort[-1]
                    self._transactions_to_abort.pop()
            if transaction is not None:
                transaction.abort()
            else:
                time.sleep(10)


_transaction_aborter = None


def add_transaction_to_abort(transaction):
    global _transaction_aborter
    if sys.is_finalizing():
        return
    if _transaction_aborter is None:
        _transaction_aborter = _TransactionAborter()
        _transaction_aborter.daemon = True
        _transaction_aborter.start()
    _transaction_aborter.add(transaction)
