from common import bool_to_string, get_value, YtError
from table import prepare_path
from tree_commands import get
from transaction_commands import _make_formatted_transactional_request

import time
from datetime import timedelta, datetime

def lock(path, mode=None, waitable=False, wait_for=None):
    """
    Tries to lock the path. Raise exception if node already under exclusive lock.
    """
    if wait_for is not None:
        wait_for = timedelta(milliseconds=wait_for)

    lock_id = _make_formatted_transactional_request(
        "lock",
        {
            "path": prepare_path(path),
            "mode": get_value(mode, "exclusive"),
            "waitable": bool_to_string(waitable)
        },
        format=None)

    if waitable and wait_for is not None and lock_id != "0-0-0-0":
        now = datetime.now()
        acquired = False
        while datetime.now() - now < wait_for:
            if get("#%s/@state" % lock_id) == "acquired":
                acquired = True
                break
            time.sleep(1.0)
        if not acquired:
            raise YtError("Timed out while waiting {0} milliseconds for lock {1}".format(wait_for.microseconds / 1000 + wait_for.seconds * 1000, lock_id))

    return lock_id

