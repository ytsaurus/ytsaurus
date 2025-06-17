from .driver import make_request
from .common import set_param


def alter_replication_card(
        replication_card_id,
        replicated_table_options=None,
        enable_replicated_table_tracker=None,
        replication_card_collocation_id=None,
        collocation_options=None,
        client=None):
    """Changes mode and enables or disables a table replica. Also manipulates with card's collocation.

    :param str replication_card_id: replication card id.
    :param dict replicated_table_options: replicated table tracker options.
        Cannot be specified with enable_replicated_table_tracker at the same time.
    :param bool enable_replicated_table_tracker: enable or disable replicated table tracker.
        Cannot be specified with replicated_table_options at the same time.
    :param str replication_card_collocation_id: id of collocation to set. Set "0-0-0-0" to remove card from collocation.
    :param str collocation_options: options to set for card's collocation. Card is expected to be a member of a collocation.

    """

    params = {"replication_card_id": replication_card_id}
    set_param(params, "replicated_table_options", replicated_table_options)
    set_param(params, "enable_replicated_table_tracker", enable_replicated_table_tracker)
    set_param(params, "replication_card_collocation_id", replication_card_collocation_id)
    set_param(params, "collocation_options", collocation_options)

    return make_request("alter_replication_card", params, client=client)


def ping_chaos_lease(
        chaos_lease_id,
        client=None):
    """Ping chaos lease.

    :param str chaos_lease_id: chaos lease id.
    """
    params = {"chaos_lease_id": chaos_lease_id}

    return make_request("ping_chaos_lease", params, client=client)
