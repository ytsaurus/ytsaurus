import config
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

def _make_transactional_request(command_name, params, **kwargs):
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
    _make_transactional_request("lock", {"path": path})

