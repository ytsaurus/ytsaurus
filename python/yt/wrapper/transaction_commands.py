import config
import logger
from http import make_request
from common import update, bool_to_string, get_value

from copy import deepcopy

def transaction_params(transaction=None, ping_ancestor_transactions=None):
    if transaction is None: transaction = config.TRANSACTION
    if ping_ancestor_transactions is None: ping_ancestor_transactions = config.PING_ANSECTOR_TRANSACTIONS
    return {"ping_ancestor_transactions": bool_to_string(ping_ancestor_transactions),
            "transaction_id": transaction}

def _add_transaction_params(params):
    return update(deepcopy(params), transaction_params())

def _make_transactioned_request(command_name, params, **kwargs):
    return make_request(command_name, _add_transaction_params(params), **kwargs)

def start_transaction(parent_transaction=None, ping_ansector_transactions=None, timeout=None):
    params = transaction_params(parent_transaction, ping_ansector_transactions)
    params["timeout"] = get_value(timeout, config.TRANSACTION_TIMEOUT)
    return make_request("start_tx", params)

def abort_transaction(transaction, ping_ansector_transactions=None):
    make_request("abort_tx", transaction_params(transaction, ping_ansector_transactions))

def commit_transaction(transaction, ping_ansector_transactions=None):
    make_request("commit_tx", transaction_params(transaction, ping_ansector_transactions))

def renew_transaction(transaction, ping_ansector_transactions=None):
    make_request("renew_tx", transaction_params(transaction, ping_ansector_transactions))

def lock(path):
    """
    Tries to lock the path. Raise exception if node already under exclusive lock.
    """
    _make_transactioned_request("lock", {"path": path})

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

    def __init__(self, timeout=None):
        if not Transaction.stack:
            Transaction.initial_transaction = config.TRANSACTION
            Transaction.initial_ping_ansector_transactions = config.PING_ANSECTOR_TRANSACTIONS
        Transaction.stack.append(start_transaction(timeout=timeout))
        self._update_global_config()

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        transaction = Transaction.stack.pop()
        if type is None:
            commit_transaction(transaction)
        else:
            logger.warning(
                "Error: (type=%s, value=%s, traceback=%s), aborting transaction %s ...",
                type,
                value,
                traceback,
                transaction)
            abort_transaction(transaction)

        self._update_global_config()

    def _update_global_config(self):
        if Transaction.stack:
            config.TRANSACTION = Transaction.stack[-1]
            config.PING_ANSECTOR_TRANSACTIONS = True
        else:
            config.TRANSACTION = Transaction.initial_transaction
            config.PING_ANSECTOR_TRANSACTIONS = Transaction.initial_ping_ansector_transactions
