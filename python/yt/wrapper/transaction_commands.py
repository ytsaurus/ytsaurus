import config
from driver import make_request, make_formatted_request
from common import update, bool_to_string, get_value
from copy import deepcopy

def transaction_params(transaction=None, ping_ancestor_transactions=None, client=None):
    client = get_value(client, config.CLIENT)
    if client is not None:
        client_transaction, client_ping_ancestor_transactions = client._get_transaction()
        transaction = get_value(transaction, client_transaction)
        ping_ancestor_transactions = get_value(ping_ancestor_transactions, client_ping_ancestor_transactions)
    else:
        transaction = get_value(transaction, config.TRANSACTION)
        ping_ancestor_transactions = get_value(ping_ancestor_transactions, config.PING_ANCESTOR_TRANSACTIONS)
    return {"ping_ancestor_transactions": bool_to_string(ping_ancestor_transactions),
            "transaction_id": transaction}

def _add_transaction_params(params):
    return update(deepcopy(params), transaction_params())

def _make_transactional_request(command_name, params, **kwargs):
    return make_request(command_name, _add_transaction_params(params), **kwargs)

def _make_formatted_transactional_request(command_name, params, format, **kwargs):
    return make_formatted_request(command_name, _add_transaction_params(params), format, **kwargs)

def start_transaction(parent_transaction=None, ping_ancestor_transactions=None, timeout=None, attributes=None, client=None):
    params = transaction_params(parent_transaction, ping_ancestor_transactions)
    timeout = get_value(timeout, config.TRANSACTION_TIMEOUT)
    if timeout is not None:
        params["timeout"] = int(timeout)
    params["attributes"] = get_value(attributes, {})
    return make_formatted_request("start_tx", params, None, client=client)

def abort_transaction(transaction, ping_ancestor_transactions=None, client=None):
    make_request("abort_tx", transaction_params(transaction, ping_ancestor_transactions), client=client)

def commit_transaction(transaction, ping_ancestor_transactions=None, client=None):
    make_request("commit_tx", transaction_params(transaction, ping_ancestor_transactions), client=client)

def ping_transaction(transaction, ping_ancestor_transactions=None, client=None):
    make_request("ping_tx", transaction_params(transaction, ping_ancestor_transactions), client=client)

