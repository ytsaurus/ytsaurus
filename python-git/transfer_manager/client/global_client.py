from .client import TransferManager

import os

_tm_client = None

def init_client(*args, **kwargs):
    global _tm_client

    url = kwargs.pop("url", None)
    if url is None:
        url = os.environ.get("YT_TRANSFER_MANAGER_URL", None)
    kwargs["url"] = url

    _tm_client = TransferManager(*args, **kwargs)

def _get_client():
    if _tm_client is None:
        init_client()
    return _tm_client

def add_task(*args, **kwargs):
    return _get_client().add_task(*args, **kwargs)

def add_tasks(*args, **kwargs):
    return _get_client().add_tasks(*args, **kwargs)

def add_tasks_from_src_dst_pairs(*args, **kwargs):
    return _get_client().add_tasks_from_src_dst_pairs(*args, **kwargs)

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

def match_src_dst_pattern(*args, **kwargs):
    return _get_client().match_src_dst_pattern(*args, **kwargs)
