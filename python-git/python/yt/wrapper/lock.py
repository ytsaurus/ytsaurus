from common import bool_to_string, get_value, YtError
from table import prepare_path
from tree_commands import get
from transaction_commands import _make_transactional_request

from yt.yson.convert import json_to_yson

import time
import simplejson as json
from datetime import timedelta, datetime

def lock(path, mode=None, waitable=False, wait_for=None, client=None):
    """Try to lock the path.

    :param mode: (optional) blocking type ["snapshot", "shared" or "exclusive" (default)]
    :param waitable: (bool) wait for lock if node is under blocking
    :param wait_for: (int) wait interval in milliseconds. If timeout occurred, `YtError` raised
    :return: taken lock id (YSON string) or ``None`` if lock was not taken.

    .. seealso:: `lock on wiki <https://wiki.yandex-team.ru/yt/userdoc/transactions#versionirovanieiloki>`_
    """
    if wait_for is not None:
        wait_for = timedelta(milliseconds=wait_for)

    lock_id = _make_transactional_request(
        "lock",
        {
            "path": prepare_path(path, client=client),
            "mode": get_value(mode, "exclusive"),
            "waitable": bool_to_string(waitable)
        },
        client=client)
    if not lock_id:
        return None
    else:
        lock_id = json_to_yson(json.loads(lock_id))

    if waitable and wait_for is not None and lock_id != "0-0-0-0":
        now = datetime.now()
        acquired = False
        while datetime.now() - now < wait_for:
            if get("#%s/@state" % lock_id, client=client) == "acquired":
                acquired = True
                break
            time.sleep(1.0)
        if not acquired:
            raise YtError("Timed out while waiting {0} milliseconds for lock {1}".format(wait_for.microseconds / 1000 + wait_for.seconds * 1000, lock_id))

    return lock_id
