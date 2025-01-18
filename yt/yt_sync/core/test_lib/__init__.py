import time
from typing import Callable
from typing import Type

from yt.yt_sync.core.client import MockOperation  # noqa:reexport
from yt.yt_sync.core.client import MockResult  # noqa:reexport

from .client_factory import get_test_yt_client_factory  # noqa:reexport
from .client_faulty import FaultyYtClientFactory  # noqa:reexport
from .client_faulty import FaultyYtClientProxy  # noqa:reexport
from .client_mock import CallTracker  # noqa:reexport
from .client_mock import MockYtClientFactory  # noqa:reexport
from .client_mock import MockYtClientProxy  # noqa:reexport
from .table_settings_builder import TableSettingsBuilder  # noqa:reexport
from .yt_errors import yt_access_denied  # noqa:reexport
from .yt_errors import yt_faulty_error  # noqa:reexport
from .yt_errors import yt_generic_error  # noqa:reexport
from .yt_errors import yt_resolve_error  # noqa:reexport
from .yt_errors import yt_retryable_error  # noqa:reexport


def wait(predicate: Callable[[], bool], timeout: int = 30) -> bool:
    iter_delay = 0.33
    iter_limit = int(timeout / iter_delay)

    iter_count = 0
    while not predicate():
        iter_count += 1
        if iter_count > iter_limit:
            return False
        time.sleep(iter_delay)
    return True


def wait_until_success(predicate: Callable[[], None], error_type: Type[Exception], timeout: int = 30):
    iter_delay = 0.33
    iter_limit = int(timeout / iter_delay)

    for i in range(iter_limit + 1):
        try:
            predicate()
            return
        except error_type as e:
            if i == iter_limit:
                raise e
            time.sleep(iter_delay)
