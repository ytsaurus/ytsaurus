from .cluster_configuration import modify_cluster_configuration, NODE_MEMORY_LIMIT_ADDITION

from yt.environment import YTInstance
from yt.environment.api import LocalYtConfig
import yt.environment.init_operation_archive as yt_env_init_operation_archive
from yt.environment.helpers import wait_for_removing_file_lock, is_file_locked, is_dead_or_zombie, yatest_common
from yt.wrapper.common import generate_uuid, GB
from yt.common import YtError, require, is_process_alive


import yt.yson as yson
import yt.json_wrapper as json

from yt.packages.six.moves import map as imap, filter as ifilter

import yt.wrapper as yt

import os
import signal
import errno
import logging
import shutil
import socket
import time
import codecs
from functools import partial

logger = logging.getLogger("Yt.local")

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
    except:
        return default
    return value == 1

def _get_attributes_from_local_dir(local_path, meta_files_suffix):
    meta_file_path = os.path.join(local_path, meta_files_suffix)
    if os.path.isfile(meta_file_path):
        with open(meta_file_path, "rb") as f:
            try:
                meta = yson.load(f)
            except yson.YsonError:
                logger.exception("Failed to load meta file {0}, meta will not be processed".format(meta_file_path))
                return {}
            return meta.get("attributes", {})
    return {}

def _create_map_node_from_local_dir(local_path, dest_path, meta_files_suffix, client):
    attributes = _get_attributes_from_local_dir(local_path, meta_files_suffix)
    client.create("map_node", dest_path, attributes=attributes, ignore_existing=True)

def _create_node_from_local_file(local_filename, dest_filename, meta_files_suffix, client):
    if not os.path.isfile(local_filename + meta_files_suffix):
        logger.warning("Found file {0} without meta info, skipping".format(local_filename))
        return

    with open(local_filename + meta_files_suffix, "rb") as f:
        try:
            meta = yson.load(f)
        except yson.YsonError:
            logger.exception("Failed to load meta file for table {0}, skipping".format(local_filename))
            return

        attributes = meta.get("attributes", {})

        if meta["type"] == "table":
            if "format" not in meta:
                logger.warning("Found table {0} with unspecified format".format(local_filename))
                return

            sorted_by = attributes.pop("sorted_by", [])

            client.create("table", dest_filename, attributes=attributes)
            with open(local_filename, "rb") as table_file:
                client.write_table(dest_filename, table_file, format=meta["format"], raw=True)

            if sorted_by:
                client.run_sort(dest_filename, sort_by=sorted_by)

        elif meta["type"] == "file":
            client.create("file", dest_filename, attributes=attributes)
            with open(local_filename, "rb") as local_file:
                client.write_file(dest_filename, local_file)

        else:
            logger.warning("Found file {0} with currently unsupported type {1}"
                           .format(local_filename, meta["type"]))

def _synchronize_cypress_with_local_dir(local_cypress_dir, meta_files_suffix, client):
    cypress_path_prefix = "//"

    if meta_files_suffix is None:
        meta_files_suffix = ".meta"

    local_cypress_dir = os.path.abspath(local_cypress_dir)
    require(os.path.exists(local_cypress_dir),
            lambda: YtError("Local Cypress directory does not exist"))

    root_attributes = _get_attributes_from_local_dir(local_cypress_dir, meta_files_suffix)
    for key in root_attributes:
        client.set_attribute("/", key, root_attributes[key])

    for root, dirs, files in os.walk(local_cypress_dir):
        rel_path = os.path.abspath(root)[len(local_cypress_dir) + 1:]  # +1 to skip last /
        for dir in dirs:
            _create_map_node_from_local_dir(os.path.join(root, dir),
                                            os.path.join(cypress_path_prefix, rel_path, dir),
                                            meta_files_suffix,
                                            client)
        for file in files:
            if file.endswith(meta_files_suffix):
                continue
            _create_node_from_local_file(os.path.join(root, file),
                                         os.path.join(cypress_path_prefix, rel_path, file),
                                         meta_files_suffix,
                                         client)

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
            logger.info("UI address: http://yt.yandex.net/%s", environment.get_proxy_address())
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
          rpc_proxy_config=None,
          master_config=None,
          node_config=None,
          scheduler_config=None,
          proxy_config=None,
          controller_agent_config=None,
          http_proxy_ports=None,
          http_proxy_count=1,
          rpc_proxy_ports=None,
          id=None,
          local_cypress_dir=None,
          enable_debug_logging=False,
          enable_logging_compression=False,
          tmpfs_path=None,
          port_range_start=None,
          listen_port_pool=None,
          fqdn=None,
          path=None,
          prepare_only=False,
          jobs_memory_limit=None,
          jobs_cpu_limit=None,
          jobs_user_slot_count=None,
          jobs_resource_limits=None,
          node_chunk_store_quota=7*GB,
          allow_chunk_storage_in_tmpfs=True,
          wait_tablet_cell_initialization=False,
          init_operations_archive=False,
          meta_files_suffix=None,
          set_pdeath_sig=False,
          watcher_config=None,
          cell_tag=0,
          ytserver_all_path=None,
          secondary_master_cell_count=0,
          log_compression_method=None):
    require(master_count >= 1, lambda: YtError("Cannot start local YT instance without masters"))

    path = get_root_path(path)
    sandbox_id = id if id is not None else generate_uuid()
    require("/" not in sandbox_id, lambda: YtError('Instance id should not contain path separator "/"'))

    sandbox_path = os.path.join(path, sandbox_id)
    sandbox_tmpfs_path = os.path.join(tmpfs_path, sandbox_id) if tmpfs_path else None

    modify_configs_func = partial(
        modify_cluster_configuration,
        master_config_patch=_load_config(master_config),
        scheduler_config_patch=_load_config(scheduler_config),
        controller_agent_config_patch=_load_config(controller_agent_config),
        node_config_patch=_load_config(node_config),
        rpc_proxy_config_patch=_load_config(rpc_proxy_config),
        proxy_config_patch=_load_config(proxy_config, is_proxy_config=True))

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

    yt_config = LocalYtConfig(
        master_count=master_count,
        node_count=node_count,
        scheduler_count=scheduler_count,
        controller_agent_count=controller_agent_count,
        secondary_cell_count=secondary_master_cell_count,
        rpc_proxy_count=rpc_proxy_count,
        http_proxy_count=http_proxy_count,
        http_proxy_ports=http_proxy_ports,
        rpc_proxy_ports=rpc_proxy_ports,
        enable_debug_logging=enable_debug_logging,
        enable_log_compression=enable_logging_compression,
        log_compression_method=log_compression_method,
        port_range_start=port_range_start,
        jobs_resource_limits=jobs_resource_limits,
        listen_port_pool=listen_port_pool,
        fqdn=fqdn or socket.getfqdn(),
        node_memory_limit_addition=NODE_MEMORY_LIMIT_ADDITION,
        primary_cell_tag=cell_tag,
        allow_chunk_storage_in_tmpfs=allow_chunk_storage_in_tmpfs,
        initialize_world=True,
        wait_tablet_cell_initialization=wait_tablet_cell_initialization,
        init_operations_archive=init_operations_archive)

    environment = YTInstance(
        sandbox_path,
        yt_config,
        preserve_working_dir=True,
        tmpfs_path=sandbox_tmpfs_path,
        modify_configs_func=modify_configs_func,
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

        # FIXME(asaitgalin): Remove this when st/YT-3054 is done.
        if not environment._load_existing_environment:
            client = environment.create_client()
            if local_cypress_dir is not None:
                _synchronize_cypress_with_local_dir(local_cypress_dir, meta_files_suffix, client)

    log_started_instance_info(environment, http_proxy_count > 0,rpc_proxy_count > 0, prepare_only)
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
            if is_dead_or_zombie(pid):
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
