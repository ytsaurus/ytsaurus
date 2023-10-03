import atexit
import exts.func
import grpc
import six
import threading

from devtools.local_cache.toolscache.proto import tools_pb2, tools_pb2_grpc
from devtools.local_cache.psingleton.proto import known_service_pb2, known_service_pb2_grpc
from devtools.local_cache.psingleton.python.systemptr import get_my_name


class _State:
    _STUB = None
    _STUB_SERVER = None
    _ADDRESS = None
    _LOCK = threading.Lock()
    _CHANNEL = None


def _get_client(paddress, server=False):
    if _State._ADDRESS == paddress:
        return _State._STUB_SERVER if server else _State._STUB

    if _State._CHANNEL is not None:
        _State._CHANNEL.close()

    _, _, address = paddress

    _State._CHANNEL = grpc.insecure_channel(address)
    _State._STUB = tools_pb2_grpc.TToolsCacheStub(_State._CHANNEL)
    _State._STUB_SERVER = known_service_pb2_grpc.TUserServiceStub(_State._CHANNEL)
    _State._ADDRESS = paddress
    return _State._STUB_SERVER if server else _State._STUB


@atexit.register
def _cleanup():
    if _State._CHANNEL is not None:
        _State._CHANNEL.close()


@exts.func.memoize()
def _memoize_my_name():
    pid, stime = get_my_name()
    return known_service_pb2.TProc(Pid=pid, StartTime=stime)


def _check_common_args(paddress, timeout, wait_for_ready, task_id=b""):
    pid, ctime, address = paddress
    assert isinstance(pid, int)
    assert isinstance(ctime, six.integer_types)
    assert isinstance(address, six.binary_type)

    assert timeout is None or isinstance(timeout, (int, float))
    assert isinstance(wait_for_ready, bool)
    assert isinstance(task_id, six.binary_type)


def notify_resource_used(paddress, sbpath, sbid, pattern=b"", bottle=b"", task_id=b"", timeout=None, wait_for_ready=False):
    task_id = six.ensure_binary(task_id)
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress
    sbpath = six.ensure_binary(sbpath)
    pattern = six.ensure_binary(pattern)
    sbid = six.ensure_binary(sbid)
    bottle = six.ensure_binary(bottle)
    assert isinstance(sbpath, six.binary_type)
    assert isinstance(pattern, six.binary_type)
    assert isinstance(sbid, six.binary_type)
    assert isinstance(bottle, six.binary_type)

    query = tools_pb2.TResourceUsed(
        Peer=known_service_pb2.TPeer(
            TaskGSID=task_id,
            Proc=_memoize_my_name()
        ),
        Pattern=pattern,
        Bottle=bottle,
        Resource=tools_pb2.TSBResource(
            Path=sbpath,
            SBId=sbid
        )
    )
    if not timeout:
        wait_for_ready = False

    with _State._LOCK:
        return _get_client(paddress).Notify(query, timeout=timeout, wait_for_ready=wait_for_ready)


def force_gc(paddress, disk_limit, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress
    assert isinstance(disk_limit, six.integer_types)

    query = tools_pb2.TForceGC(
        Peer=known_service_pb2.TPeer(
            TaskGSID=task_id,
            Proc=_memoize_my_name()
        ),
        TargetSize=disk_limit
    )

    if not timeout:
        wait_for_ready = False

    with _State._LOCK:
        return _get_client(paddress).ForceGC(query, timeout=timeout, wait_for_ready=wait_for_ready)


def lock_resource(paddress, sbpath, sbid, timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready)
    pid, ctime, address = paddress
    sbpath = six.ensure_binary(sbpath)
    sbid = six.ensure_binary(sbid)

    query = tools_pb2.TSBResource(
        Path=sbpath,
        SBId=sbid
    )
    if not timeout:
        wait_for_ready = False

    with _State._LOCK:
        return _get_client(paddress).LockResource(query, timeout=timeout, wait_for_ready=wait_for_ready)


def get_task_stats(paddress, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress

    query = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )
    if not timeout:
        wait_for_ready = False

    with _State._LOCK:
        return _get_client(paddress).GetTaskStats(query, timeout=timeout, wait_for_ready=wait_for_ready)


def check_status(paddress, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress

    query = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )

    if not timeout:
        wait_for_ready = False

    with _State._LOCK:
        return _get_client(paddress, server=True).GetStatus(query, timeout=timeout, wait_for_ready=wait_for_ready)


def unlock_all(paddress, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress

    query = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )

    if not timeout:
        wait_for_ready = False

    with _State._LOCK:
        return _get_client(paddress).UnlockAllResources(query, timeout=timeout, wait_for_ready=wait_for_ready)
