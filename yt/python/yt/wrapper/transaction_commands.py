from .config import get_config, get_command_param
from .driver import make_request, make_formatted_request, get_api_version
from .common import update, get_value, get_started_by, set_param, datetime_to_string
from .batch_response import apply_function_to_result

from datetime import datetime


def transaction_params(transaction, client=None):
    params = {}
    set_param(params, "transaction_id", get_value(transaction, get_command_param("transaction_id", client)))
    set_param(params, "ping_ancestor_transactions", get_command_param("ping_ancestor_transactions", client))
    set_param(params, "suppress_transaction_coordinator_sync", get_command_param("suppress_transaction_coordinator_sync", client))
    return params


def start_transaction(parent_transaction=None, timeout=None, deadline=None, attributes=None, type="master",
                      sticky=False, prerequisite_transaction_ids=None, client=None):
    """Starts transaction.

    :param str parent_transaction: parent transaction id.
    :param int timeout: transaction lifetime since last ping in milliseconds.
    :param str type: could be either "master" or "tablet"
    :param bool sticky: EXPERIMENTAL, do not use it, unless you have been told to do so.
    :param dict attributes: attributes
    :return: new transaction id.
    :rtype: str

    .. seealso:: `start_tx in the docs <https://ytsaurus.tech/docs/en/api/commands#start_tx>`_
    """
    params = transaction_params(parent_transaction, client=client)
    timeout = get_value(timeout, get_config(client)["transaction_timeout"])
    set_param(params, "timeout", timeout, int)
    set_param(params, "deadline", deadline, lambda dt: datetime_to_string(dt) if isinstance(dt, datetime) else dt)
    params["attributes"] = update(get_started_by(), get_value(attributes, {}))
    params["type"] = type
    params["sticky"] = sticky
    set_param(params, "prerequisite_transaction_ids", prerequisite_transaction_ids)

    command_name = "start_transaction" if get_api_version(client) == "v4" else "start_tx"

    def _process_result(request_result):
        return request_result["transaction_id"] if get_api_version(client) == "v4" else request_result

    return apply_function_to_result(_process_result, make_formatted_request(command_name, params, None, client=client))


def abort_transaction(transaction, client=None):
    """Aborts transaction. All changes will be lost.

    :param str transaction: transaction id.

    .. seealso:: `abort_tx in the docs <https://ytsaurus.tech/docs/en/api/commands#abort_tx>`_
    """
    params = transaction_params(transaction, client=client)
    command_name = "abort_transaction" if get_api_version(client) == "v4" else "abort_tx"
    return make_request(command_name, params, client=client)


def commit_transaction(transaction, client=None):
    """Saves all transaction changes.

    :param str transaction: transaction id.

    .. seealso:: `commit_tx in the docs <https://ytsaurus.tech/docs/en/api/commands#commit_tx>`_
    """
    params = transaction_params(transaction, client=client)
    command_name = "commit_transaction" if get_api_version(client) == "v4" else "commit_tx"
    return make_request(command_name, params, client=client)


def ping_transaction(transaction, timeout=None, retry_config=None, client=None):
    """Prolongs transaction lifetime.

    :param str transaction: transaction id.

    .. seealso:: `ping_tx in the docs <https://ytsaurus.tech/docs/en/api/commands#ping_tx>`_
    """
    params = transaction_params(transaction, client=client)
    command_name = "ping_transaction" if get_api_version(client) == "v4" else "ping_tx"
    return make_request(command_name, params, timeout=timeout, retry_config=retry_config, client=client)
