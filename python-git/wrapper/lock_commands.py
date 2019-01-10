from .common import get_value, YtError, set_param
from .ypath import YPath
from .cypress_commands import get
from .transaction_commands import _make_transactional_request, _make_formatted_transactional_request

import time
from datetime import timedelta, datetime

def _is_batch_client(client):
    # XXX: Import inside function to avoid import loop.
    # batch_client imports all API, including current module
    from .batch_client import BatchClient

    return isinstance(client, BatchClient)

def lock(path, mode=None, waitable=False, wait_for=None, child_key=None, attribute_key=None, client=None):
    """Tries to lock the path.

    :param str mode: blocking type, one of ["snapshot", "shared" or "exclusive"], "exclusive" by default.
    :param bool waitable: wait for lock if node is under blocking.
    :param int wait_for: wait interval in milliseconds. If timeout occurred, \
    :class:`YtError <yt.common.YtError>` is raised.
    :return: taken lock id (as :class:`YsonString <yt.yson.yson_types.YsonString>`) or throws \
    :class:`YtResponseError <yt.wrapper.errors.YtResponseError>` with 40* code if lock conflict detected.

    .. seealso:: `lock on wiki <https://wiki.yandex-team.ru/yt/userdoc/transactions#versionirovanieiloki>`_
    """
    if wait_for is not None:
        wait_for = timedelta(milliseconds=wait_for)

    if waitable and wait_for is not None:
        if _is_batch_client(client):
            raise YtError("Waiting on waitable locks with batch client is not supported")

    params = {
        "path": YPath(path, client=client),
        "mode": get_value(mode, "exclusive"),
        "waitable": waitable}

    set_param(params, "child_key", child_key)
    set_param(params, "attribute_key", attribute_key)

    lock_id = _make_formatted_transactional_request("lock", params, format=None, client=client)
    if not lock_id:
        return None

    if waitable and wait_for is not None and lock_id != "0-0-0-0":
        now = datetime.now()
        acquired = False
        while datetime.now() - now < wait_for:
            if get("#%s/@state" % lock_id, client=client) == "acquired":
                acquired = True
                break
            time.sleep(1.0)
        if not acquired:
            raise YtError(
                "Timed out while waiting {0} milliseconds for lock {1}"
                .format(wait_for.microseconds // 1000 + wait_for.seconds * 1000, lock_id))

    return lock_id

def unlock(path, client=None):
    """Tries to unlock the path.

    Both acquired and pending locks are unlocked. Only explicit locks are unlockable.

    If the node is not locked, succeeds silently. If the locked version of the node
    contains changes compared to its original version, :class:`YtError <yt.common.YtError>` is raised.

    .. seealso:: `unlock on wiki <https://wiki.yandex-team.ru/yt/userdoc/transactions#versionirovanieiloki>`_
    """
    params = {"path": YPath(path, client=client)}
    _make_transactional_request("unlock", params, client=client)
