from client import TransferManager

from yt.common import get_value

import os

_tm_client = None

def init_client(url=None, token=None):
    global _tm_client
    backend_url = get_value(url, os.environ.get("YT_TRANSFER_MANAGER_URL", None))
    _tm_client = TransferManager(url=backend_url, token=token)

def _get_client():
    if _tm_client is None:
        init_client()
    return _tm_client

def add_task(*args, **kwargs):
    return _get_client().add_task(*args, **kwargs)

def add_tasks(*args, **kwargs):
    return _get_client().add_tasks(*args, **kwargs)

def abort_task(*args, **kwargs):
    return _get_client().abort_task(*args, **kwargs)

def restart_task(*args, **kwargs):
    return _get_client().restart_task(*args, **kwargs)

def get_task_info(*args, **kwargs):
    return _get_client().get_task_info(*args, **kwargs)

def get_tasks(*args, **kwargs):
    return _get_client().get_tasks(*args, **kwargs)

def get_backend_config(*args, **kwargs):
    return _get_client().get_backend_config(*args, **kwargs)

