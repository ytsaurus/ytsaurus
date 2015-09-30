"""
Python client for HTTP-interface of Transfer Manager.

Package supports `Transfer Manager API <https://wiki.yandex-team.ru/yt/userdoc/transfermanager>`_.

Be ready to catch :py:exc:`yt.wrapper.errors.YtError` after all commands!
"""

from client import TransferManager
from global_client import add_task, add_tasks, abort_task, restart_task, get_task_info, get_tasks, get_backend_config
