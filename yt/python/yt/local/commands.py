from yt.environment import YTInstance
from yt.environment.api import LocalYtConfig
from yt.environment.helpers import wait_for_removing_file_lock, is_file_locked, is_dead, yatest_common
from yt.wrapper.constants import LOCAL_MODE_URL_PATTERN
from yt.wrapper.common import generate_uuid, GB, MB
from yt.common import YtError, require, is_process_alive, get_fqdn


import yt.yson as yson
import yt.json_wrapper as json

try:
    from yt.packages.six.moves import map as imap, filter as ifilter
except ImportError:
    from six.moves import map as imap, filter as ifilter

import yt.wrapper as yt

import os
import signal
import errno
import logging
import shutil
import time
import codecs

logger = logging.getLogger("YtLocal")

YT_LOCAL_STOP_WAIT_TIME = 5


def _load_config(config, is_proxy_config=False):
    if config is None:
        return {}

    if isinstance(config, dict):
        return config

    path = config
    with open(path, "rb") as fin:
        if not is_proxy_config:
            return yson.load(fin)
        else:
            return json.load(codecs.getreader("utf-8")(fin))


def get_root_path(path=None):
    if path is not None:
        return path
    else:
        return os.environ.get("YT_LOCAL_ROOT_PATH", os.getcwd())


def get_main_process_pid_file_path(path):
    return os.path.join(path, "main_process_pid.txt")


def touch(path):
    open(path, 'a').close()


def _get_bool_from_env(name, default=False):
    value = os.environ.get(name, None)
    if value is None:
        return default
    try:
        value = int(value)
    except:  # noqa
        return default
    return value == 1


def _read_pids_file(pids_file_path):
    with open(pids_file_path) as f:
        return list(imap(int, f))


def log_started_instance_info(environment, start_proxy, start_rpc_proxy, prepare_only):
    logger.info("Local YT {0}, id: {1}".format(
        "prepared" if prepare_only else "started",
        environment.id))
    if start_proxy:
        logger.info("HTTP proxy addresses: %s", environment.get_http_proxy_addresses())
        if "localhost" not in environment.get_proxy_address():
            logger.info("UI address: http://%s", LOCAL_MODE_URL_PATTERN.format(local_mode_address=environment.get_proxy_address()))
    if start_rpc_proxy:
        logger.info("GRPC proxy address: %s", environment.get_grpc_proxy_address())


def _safe_kill(pid, signal_number=signal.SIGKILL):
    try:
        os.killpg(pid, signal_number)
    except OSError as err:
        if err.errno == errno.EPERM:
            logger.error("Failed to kill process with pid {0}, access denied".format(pid))
        elif err.errno == errno.ESRCH:
            logger.warning("Failed to kill process with pid {0}, process not found".format(pid))
        else:
            # According to "man 2 killpg" possible error values are
            # (EINVAL, EPERM, ESRCH)
            raise


def _safe_remove(path):
    try:
        os.remove(path)
    except OSError as err:
        if err.errno != errno.ENOENT:
            raise


def start(master_count=1,
          node_count=1,
          scheduler_count=1,
          controller_agent_count=1,
          rpc_proxy_count=0,
          master_cache_count=0,
          tablet_balancer_count=0,
          queue_agent_count=0,
          cypress_proxy_count=0,
          rpc_proxy_config=None,
          master_config=None,
          node_config=None,
          scheduler_config=None,
          proxy_config=None,
          driver_config=None,
          global_cluster_connection_config=None,
          controller_agent_config=None,
          master_cache_config=None,
          http_proxy_ports=None,
          http_proxy_count=1,
          rpc_proxy_ports=None,
          id=None,
          local_cypress_dir=None,
          enable_debug_logging=False,
          enable_structured_logging=False,
          enable_logging_compression=False,
          mock_tvm_id=None,
          tmpfs_path=None,
          port_range_start=None,
          listen_port_pool=None,
          fqdn=None,
          path=None,
          prepare_only=False,
          jobs_environment_type=None,
          jobs_memory_limit=None,
          jobs_cpu_limit=None,
          jobs_user_slot_count=None,
          jobs_resource_limits=None,
          node_chunk_store_quota=7*GB,
          allow_chunk_storage_in_tmpfs=True,
          store_location_count=1,
          wait_tablet_cell_initialization=False,
          init_operations_archive=False,
          meta_files_suffix=None,
          set_pdeath_sig=False,
          watcher_config=None,
          cell_tag=1,
          ytserver_all_path=None,
          secondary_master_cell_count=0,
          log_compression_method=None,
          enable_master_cache=False,
          clock_count=0,
          chaos_node_count=0,
          replicated_table_tracker_count=0):
    require(master_count >= 1, lambda: YtError("Cannot start local YT instance without masters"))

    path = get_root_path(path)
    sandbox_id = id if id is not None else generate_uuid()
    require("/" not in sandbox_id, lambda: YtError('Instance id should not contain path separator "/"'))

    sandbox_path = os.path.join(path, sandbox_id)
    sandbox_tmpfs_path = os.path.join(tmpfs_path, sandbox_id) if tmpfs_path else None

    watcher_config = _load_config(watcher_config)

    if jobs_resource_limits is None:
        jobs_resource_limits = {
            "memory": 16 * GB,
            "cpu": 1,
            "user_slots": 10,
        }

    if jobs_cpu_limit is not None:
        jobs_resource_limits["cpu"] = jobs_cpu_limit
    if jobs_memory_limit is not None:
        jobs_resource_limits["memory"] = jobs_memory_limit
    if jobs_user_slot_count is not None:
        jobs_resource_limits["user_slots"] = jobs_user_slot_count

    if scheduler_count == 0:
        controller_agent_count = 0

    yt_config = LocalYtConfig(
        master_count=master_count,
        clock_count=clock_count,
        node_count=node_count,
        chaos_node_count=chaos_node_count,
        scheduler_count=scheduler_count,
        controller_agent_count=controller_agent_count,
        secondary_cell_count=secondary_master_cell_count,
        rpc_proxy_count=rpc_proxy_count,
        http_proxy_count=http_proxy_count,
        master_cache_count=master_cache_count,
        tablet_balancer_count=tablet_balancer_count,
        cypress_proxy_count=cypress_proxy_count,
        replicated_table_tracker_count=replicated_table_tracker_count,
        # TODO: Should we default to a fixed name, like "primary" in integration tests?
        cluster_name=sandbox_id,
        queue_agent_count=queue_agent_count,
        delta_master_config=_load_config(master_config),
        delta_scheduler_config=_load_config(scheduler_config),
        delta_controller_agent_config=_load_config(controller_agent_config),
        delta_node_config=_load_config(node_config),
        delta_rpc_proxy_config=_load_config(rpc_proxy_config),
        delta_http_proxy_config=_load_config(proxy_config, is_proxy_config=True),
        delta_master_cache_config=_load_config(master_cache_config),
        delta_driver_config=_load_config(driver_config),
        delta_global_cluster_connection_config=_load_config(global_cluster_connection_config),

        http_proxy_ports=http_proxy_ports,
        rpc_proxy_ports=rpc_proxy_ports,
        enable_master_cache=enable_master_cache,
        enable_debug_logging=enable_debug_logging,
        enable_structured_logging=enable_structured_logging,
        enable_log_compression=enable_logging_compression,
        mock_tvm_id=mock_tvm_id,
        log_compression_method=log_compression_method,
        port_range_start=port_range_start,
        jobs_environment_type=jobs_environment_type,
        use_slot_user_id=False,
        jobs_resource_limits=jobs_resource_limits,
        listen_port_pool=listen_port_pool,
        fqdn=fqdn or get_fqdn(),
        optimize_config=True,
        node_memory_limit_addition=500*MB + 200*MB + 500*MB,
        primary_cell_tag=cell_tag,
        allow_chunk_storage_in_tmpfs=allow_chunk_storage_in_tmpfs,
        store_location_count=store_location_count,
        initialize_world=True,
        local_cypress_dir=local_cypress_dir,
        meta_files_suffix=meta_files_suffix,
        wait_tablet_cell_initialization=wait_tablet_cell_initialization,
        init_operations_archive=init_operations_archive)

    environment = YTInstance(
        sandbox_path,
        yt_config,
        preserve_working_dir=True,
        tmpfs_path=sandbox_tmpfs_path,
        kill_child_processes=set_pdeath_sig,
        watcher_config=watcher_config,
        ytserver_all_path=ytserver_all_path)

    environment.id = sandbox_id

    require(_is_stopped(sandbox_id, path),
            lambda: YtError("Instance with id {0} is already running".format(sandbox_id)))

    pids_filename = os.path.join(environment.path, "pids.txt")
    if os.path.isfile(pids_filename):
        pids = _read_pids_file(pids_filename)
        alive_pids = list(ifilter(is_process_alive, pids))
        for pid in alive_pids:
            logger.warning("Killing alive process (pid: {0}) from previously run instance".format(pid))
            _safe_kill(pid)
        os.remove(pids_filename)

    is_started_file = os.path.join(sandbox_path, "started")
    if os.path.exists(is_started_file):
        os.remove(is_started_file)

    if not prepare_only:
        environment.start()

    log_started_instance_info(environment, http_proxy_count > 0, rpc_proxy_count > 0, prepare_only)
    touch(is_started_file)

    return environment


def _is_stopped(id, path=None, verbose=False):
    sandbox_path = os.path.join(get_root_path(path), id)

    if not os.path.isdir(sandbox_path):
        return True

    lock_file_path = os.path.join(sandbox_path, "lock_file")

    # Debug info for tests.
    if yatest_common is not None:
        if verbose and not os.path.exists(lock_file_path):
            logger.warning("File %s does not exist", lock_file_path)

    if is_file_locked(lock_file_path):
        return False
    else:
        if verbose and yatest_common is not None:
            logger.warning("Lock is not acquired on %s", lock_file_path)

    return True


def _is_exists(id, path=None):
    sandbox_path = os.path.join(get_root_path(path), id)
    return os.path.isdir(sandbox_path)


def stop(id, remove_working_dir=False, remove_runtime_data=False, path=None, ignore_lock=False):
    require(_is_exists(id, path),
            lambda: yt.YtError("Local YT with id {0} not found".format(id)))
    if not ignore_lock:
        require(not _is_stopped(id, path, verbose=True),
                lambda: yt.YtError("Local YT with id {0} is already stopped".format(id)))

    sandbox_dir = os.path.join(get_root_path(path), id)
    pids_file_path = os.path.join(sandbox_dir, "pids.txt")
    main_process_pid_file = get_main_process_pid_file_path(sandbox_dir)

    if os.path.exists(main_process_pid_file):
        pid = _read_pids_file(main_process_pid_file)[0]
        _safe_kill(pid, signal_number=signal.SIGINT)

        start_time = time.time()
        killed = False

        while time.time() - start_time < YT_LOCAL_STOP_WAIT_TIME:
            if is_dead(pid):
                killed = True
                break

        if not killed:
            logger.warning("Failed to kill YT local main process with SIGINT, SIGKILL will be used")
            _safe_kill(pid, signal_number=signal.SIGKILL)

            for path in (pids_file_path, main_process_pid_file):
                _safe_remove(path)
    else:
        for pid in _read_pids_file(pids_file_path):
            _safe_kill(pid)
        os.remove(pids_file_path)

    wait_for_removing_file_lock(os.path.join(get_root_path(path), id, "lock_file"))

    if remove_working_dir:
        delete(id, force=True, path=path)
    elif remove_runtime_data:
        runtime_data_path = os.path.join(sandbox_dir, "runtime_data")
        shutil.rmtree(runtime_data_path, ignore_errors=True)


def delete(id, force=False, path=None):
    require(_is_exists(id, path) or force,
            lambda: yt.YtError("Local YT with id {0} not found".format(id)))
    require(_is_stopped(id, path),
            lambda: yt.YtError("Local YT environment with id {0} is not stopped".format(id)))
    shutil.rmtree(os.path.join(get_root_path(path), id), ignore_errors=True)


def get_proxy(id, path=None):
    require(_is_exists(id, path), lambda: yt.YtError("Local YT with id {0} not found".format(id)))

    info_file_path = os.path.join(get_root_path(path), id, "info.yson")
    require(os.path.exists(info_file_path),
            lambda: yt.YtError("Information file for local YT with id {0} not found".format(id)))

    with open(info_file_path, "rb") as f:
        info = yson.load(f)
        if "http_proxies" in info:
            return info["http_proxies"][0]["address"]
        # COMPAT(max42)
        elif "proxy" in info:
            return info["proxy"]["address"]
        else:
            raise yt.YtError("Local YT with id {0} does not have started proxy".format(id))


def list_instances(path=None):
    path = get_root_path(path)
    result = []
    for dir_ in os.listdir(path):
        full_path = os.path.join(path, dir_)
        if not os.path.isdir(full_path):
            logger.info("Found unknown object in instances root: %s", full_path)
            continue

        info_file = os.path.join(full_path, "info.yson")
        if not os.path.exists(info_file):
            logger.info("Path %s does not seem to contain valid local YT instance", full_path)
            continue

        stopped = _is_stopped(dir_, path)
        if stopped:
            result.append((dir_, "stopped", None))
        else:
            try:
                proxy_address = get_proxy(dir_, path)
            except yt.YtError:
                proxy_address = None
            result.append((dir_, "running", proxy_address))

    return result
