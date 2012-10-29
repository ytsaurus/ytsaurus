import config
from http import make_request
from common import update

from copy import deepcopy

def add_transaction_params(params):
    return update(deepcopy(params), {
        "ping_ancestor_transactions": config.PING_ANSECTOR_TRANSACTIONS,
        "transaction_id": config.TRANSACTION})


def start_transaction(parent_transaction=None, ping_ansector_transactions=False):
    if parent_transaction is None: parent_transaction = config.TRANSACTION
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": parent_transaction}
    return make_request("POST", "start_tx", params)

def abort_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": transaction}
    return make_request("POST", "abort_tx", params)

def commit_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": transaction}
    return make_request("POST", "commit_tx", params)

def renew_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": config.PING_ANSECTOR_TRANSACTIONS,
              "transaction_id": transaction}
    return make_request("POST", "renew_tx", params)
