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

def _add_transaction_params(params, client=None):
    return update(deepcopy(params), transaction_params(client=client))

def _make_transactional_request(command_name, params, **kwargs):
    return make_request(command_name, _add_transaction_params(params, kwargs.get("client", None)), **kwargs)

def _make_formatted_transactional_request(command_name, params, format, **kwargs):
    return make_formatted_request(command_name, _add_transaction_params(params, kwargs.get("client", None)), format, **kwargs)

def start_transaction(parent_transaction=None, timeout=None, attributes=None, client=None):
    """Start transaction.

    :param parent_transaction: (string) parent transaction id
    :param timeout: transaction lifetime singe last ping in milliseconds
    :param attributes: (dict)
    :return: (string) new transaction id
    .. seealso:: `start_tx on wiki <https://wiki.yandex-team.ru/yt/Design/ClientInterface/Core#starttx>`_
    """
    params = transaction_params(parent_transaction, client=client)
    timeout = get_value(timeout, config.TRANSACTION_TIMEOUT)
    if timeout is not None:
        params["timeout"] = int(timeout)
    params["attributes"] = get_value(attributes, {})
    return make_formatted_request("start_tx", params, None, client=client)

def abort_transaction(transaction, client=None):
    """Abort transaction. All changes will be lost.

    :param transaction: (string) transaction id
    .. seealso:: `abort_tx on wiki <https://wiki.yandex-team.ru/yt/Design/ClientInterface/Core#aborttx>`_
    """
    make_request("abort_tx", transaction_params(transaction, client=client), client=client)

def commit_transaction(transaction, client=None):
    """Save all transaction changes.

    :param transaction: (string) transaction id
    .. seealso:: `commit_tx on wiki <https://wiki.yandex-team.ru/yt/Design/ClientInterface/Core#committx>`_
    """
    make_request("commit_tx", transaction_params(transaction, client=client), client=client)

def ping_transaction(transaction, client=None):
    """Prolong transaction lifetime.

    :param transaction: (string) transaction id
    .. seealso:: `ping_tx on wiki <https://wiki.yandex-team.ru/yt/Design/ClientInterface/Core#pingtx>`_
    """
    make_request("ping_tx", transaction_params(transaction, client=client), client=client)
