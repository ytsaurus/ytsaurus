import atexit
import contextlib
import exts.func
import grpc
import logging
import six
import threading

from devtools.local_cache.ac.proto import ac_pb2, ac_pb2_grpc
from devtools.local_cache.psingleton.proto import known_service_pb2
from devtools.local_cache.psingleton.python.systemptr import get_my_name


_CHANNELS = {}
_LOCK = threading.Lock()
_LAST_ADDRESS = None
_REF_COUNTS = {}
_RECREATE_CHECK_REPORTED = False

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def _get_client(paddress):
    _OPTIONS = [
        # ("grpc.keepalive_time_ms", 10000),
        # ("grpc.keepalive_timeout_ms", 10000),
        # ("grpc.keepalive_permit_without_calls", 1),
        # ("grpc.max_connection_idle_ms", 200000),
        # ("grpc.max_connection_age_ms", 200000),
        # ("grpc.client_idle_timeout_ms", 200000),
        # ("grpc.http2.min_time_between_pings_ms", 1000),
        # ("grpc.http2.min_ping_interval_without_data_ms", 1000),
        # ("grpc.minimal_stack", 1),
        # ("grpc.optimization_target", "latency"),
    ]

    with _LOCK:
        client, channel = _CHANNELS.get(paddress, (None, None))
        if client is None:
            _, _, new_address = paddress
            channel = grpc.insecure_channel(new_address, options=_OPTIONS)
            client = ac_pb2_grpc.TACCacheStub(channel)
            _CHANNELS[paddress] = (client, channel)
            _REF_COUNTS[paddress] = 1
        else:
            _REF_COUNTS[paddress] += 1

        _LAST_ADDRESS = paddress

    yield client

    with _LOCK:
        _REF_COUNTS[paddress] -= 1

        if paddress != _LAST_ADDRESS and _REF_COUNTS[paddress] == 0:
            channel.close()
            del _CHANNELS[paddress]


@atexit.register
def _cleanup():
    for v in _CHANNELS.itervalues():
        client, channel = v
        channel.close()


@exts.func.memoize()
def _memoize_my_name():
    pid, stime = get_my_name()
    return known_service_pb2.TProc(Pid=pid, StartTime=stime)


def _analyze_metadata(call):
    global _RECREATE_CHECK_REPORTED

    if _RECREATE_CHECK_REPORTED:
        return

    for key, value in call.trailing_metadata():
        if key == 'ac-db-recreated' and value == 'true':
            logger.warn("AC cache was recreated")

    _RECREATE_CHECK_REPORTED = True


def _check_common_args(paddress, timeout, wait_for_ready, task_id=b""):
    pid, ctime, address = paddress
    assert isinstance(pid, int), type(pid)
    assert isinstance(ctime, six.integer_types), type(ctime)
    assert isinstance(address, six.binary_type), type(address)

    assert timeout is None or isinstance(timeout, (int, float)), type(timeout)
    assert isinstance(wait_for_ready, bool), type(wait_for_ready)
    assert isinstance(task_id, six.binary_type), type(task_id)


def put_uid(paddress, uid, root_path, blobs, weight, hardlink, replace, is_result, file_names=None, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress

    uid = six.ensure_binary(uid)
    root_path = six.ensure_binary(root_path)

    assert isinstance(uid, six.binary_type), type(uid)
    assert isinstance(root_path, six.binary_type), type(root_path)
    assert isinstance(blobs, list), type(blobs)
    assert isinstance(hardlink, bool), type(hardlink)
    assert isinstance(weight, int), type(weight)
    assert isinstance(replace, bool), type(replace)
    assert isinstance(is_result, bool), type(is_result)
    assert file_names is None or isinstance(file_names, list), type(file_names)
    assert file_names is None or len(file_names) == len(blobs), type(blobs)

    peer = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )

    blob_info = []
    for fpath, fuid in blobs:
        fpath = six.ensure_binary(fpath)

        assert isinstance(fpath, six.binary_type), type(fpath)
        if fuid is None:
            blob_info.append(ac_pb2.TBlobInfo(Path=fpath, Optimization=ac_pb2.Hardlink if hardlink else ac_pb2.Copy))
        else:
            fuid = six.ensure_binary(fuid)

            assert isinstance(fuid, six.binary_type), type(fuid)
            blob_info.append(ac_pb2.TBlobInfo(Path=fpath, Optimization=ac_pb2.Hardlink if hardlink else ac_pb2.Copy, CASHash=ac_pb2.THash(Uid=fuid)))

    query = ac_pb2.TPutUid(
        ACHash=ac_pb2.THash(Uid=uid),
        RootPath=root_path,
        Origin=ac_pb2.TOrigin(OriginKind=ac_pb2.User),
        BlobInfo=blob_info,
        DBFileNames=file_names or [],
        Peer=peer,
        Weight=weight,
        ReplacementMode=ac_pb2.ForceBlobReplacement if replace else ac_pb2.UseOldBlobs,
        Result=is_result
    )

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.Put.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp, call.trailing_metadata()


def get_uid(paddress, uid, dest_path, hardlink, is_result, release=False, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress

    uid = six.ensure_binary(uid)
    dest_path = six.ensure_binary(dest_path)

    assert isinstance(uid, six.binary_type), type(uid)
    assert isinstance(dest_path, six.binary_type), type(dest_path)
    assert isinstance(hardlink, bool), type(hardlink)
    assert isinstance(is_result, bool), type(is_result)
    assert isinstance(release, bool), type(release)

    peer = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )

    query = ac_pb2.TGetUid(
        ACHash=ac_pb2.THash(Uid=uid),
        DestPath=dest_path,
        Optimization=ac_pb2.Hardlink if hardlink else ac_pb2.Copy,
        Result=is_result,
        Peer=peer,
        Release=release
    )

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.Get.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp, call.trailing_metadata()


def has_uid(paddress, uid, is_result, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress

    uid = six.ensure_binary(uid)

    assert isinstance(uid, six.binary_type), type(uid)
    assert isinstance(is_result, bool), type(is_result)

    peer = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )

    query = ac_pb2.THasUid(
        ACHash=ac_pb2.THash(Uid=uid),
        Peer=peer,
        Result=is_result
    )

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.Has.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp


def remove_uid(paddress, uid, forced_removal=False, timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready)
    pid, ctime, address = paddress

    uid = six.ensure_binary(uid)

    assert isinstance(uid, six.binary_type), type(uid)
    assert isinstance(forced_removal, bool), type(forced_removal)

    query = ac_pb2.TRemoveUid(
        ACHash=ac_pb2.THash(Uid=uid),
        ForcedRemoval=forced_removal,
    )

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.Remove.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp


def get_task_stats(paddress, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)

    pid, ctime, address = paddress
    query = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )
    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.GetTaskStats.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp


def put_dependencies(paddress, uid, deps, timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready)
    pid, ctime, address = paddress

    uid = six.ensure_binary(uid)

    assert isinstance(uid, six.binary_type), type(uid)
    assert isinstance(deps, list), type(deps)

    dep_info = [ac_pb2.THash(Uid=duid) for duid in deps]

    query = ac_pb2.TNodeDependencies(
        NodeHash=ac_pb2.THash(Uid=uid),
        RequiredHashes=dep_info
    )

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.PutDeps.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp


def force_gc(paddress, disk_limit, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress
    assert isinstance(disk_limit, six.integer_types)

    query = ac_pb2.TForceGC(
        Peer=known_service_pb2.TPeer(
            TaskGSID=task_id,
            Proc=_memoize_my_name()
        ),
        TargetSize=disk_limit
    )

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.ForceGC.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp


def release_all(paddress, task_id=b"", timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready, task_id=task_id)
    pid, ctime, address = paddress

    query = known_service_pb2.TPeer(
        TaskGSID=task_id,
        Proc=_memoize_my_name()
    )

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.ReleaseAll.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp


def get_cache_stats(paddress, timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready)
    pid, ctime, address = paddress

    query = ac_pb2.TStatus()

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        return c.GetCacheStats(query, timeout=timeout, wait_for_ready=wait_for_ready)


def analyze_du(paddress, timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready)
    pid, ctime, address = paddress

    query = ac_pb2.TStatus()

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.AnalyzeDU.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp


def synchronous_gc(paddress, total_size=None, min_last_access=None, max_object_size=None, timeout=None, wait_for_ready=False):
    _check_common_args(paddress, timeout, wait_for_ready)
    pid, ctime, address = paddress

    assert total_size is None or isinstance(total_size, six.integer_types + (float, ))
    assert max_object_size is None or isinstance(max_object_size, six.integer_types + (float, ))
    assert min_last_access is None or isinstance(min_last_access, six.integer_types + (float, ))

    if total_size is not None:
        query = ac_pb2.TSyncronousGC(
            TotalSize=long(total_size) if six.PY2 else int(total_size),
        )
    elif max_object_size is not None:
        query = ac_pb2.TSyncronousGC(
            BLobSize=long(max_object_size) if six.PY2 else int(max_object_size),
        )
    elif min_last_access is not None:
        query = ac_pb2.TSyncronousGC(
            Timestamp=long(min_last_access) if six.PY2 else int(min_last_access),
        )
    else:
        query = ac_pb2.TSyncronousGC()

    if not timeout:
        wait_for_ready = False

    with _get_client(paddress) as c:
        resp, call = c.SynchronousGC.with_call(query, timeout=timeout, wait_for_ready=wait_for_ready)
        _analyze_metadata(call)
        return resp
