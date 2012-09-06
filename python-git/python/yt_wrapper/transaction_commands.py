from http import make_request


def start_transaction(parent_transaction=None, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": ping_ansector_transactions}
    if parent_transaction is not None:
        params["transaction_id"] = parent_transaction
    return make_request("POST", "start_tx", params)

def abort_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": ping_ansector_transactions,
              "transaction_id": transaction}
    return make_request("POST", "abort_tx", params)

def commit_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": ping_ansector_transactions,
              "transaction_id": transaction}
    return make_request("POST", "commit_tx", params)

def renew_transaction(transaction, ping_ansector_transactions=False):
    params = {"ping_ansector_transactions": ping_ansector_transactions,
              "transaction_id": transaction}
    return make_request("POST", "renew_tx", params)
