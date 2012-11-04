import config
from http import make_request
from common import update, bool_to_string

from copy import deepcopy

def add_transaction_params(params):
    return update(deepcopy(params), {
        "ping_ancestor_transactions": bool_to_string(config.PING_ANSECTOR_TRANSACTIONS),
        "transaction_id": config.TRANSACTION})


def start_transaction(parent_transaction=None, ping_ansector_transactions=False):
    if parent_transaction is None: parent_transaction = config.TRANSACTION
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": parent_transaction}
    return make_request("start_tx", params)

def abort_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": transaction}
    return make_request("abort_tx", params)

def commit_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": transaction}
    return make_request("commit_tx", params)

def renew_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": transaction}
    return make_request("renew_tx", params)
