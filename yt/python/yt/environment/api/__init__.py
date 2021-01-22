import attr


@attr.s
class LocalYtConfig(object):
    """High level local YT configuration"""
    inside_arcadia = attr.ib(True)
    path = attr.ib(None)
    tmpfs_path = attr.ib(None)
    local_cypress_path = attr.ib(None)
    fqdn = attr.ib("localhost")
    use_porto_for_servers = attr.ib(False)
    kill_child_processes = attr.ib(False)
    run_watcher = attr.ib(False)
    capture_stderr_to_file = attr.ib(True)
    optimize_config = attr.ib(False)

    """High level master configuration"""
    primary_cell_tag = attr.ib(None)

    """High level node settings"""
    jobs_resource_limits = attr.ib(factory=lambda: {
        "cpu": 1,
        "memory": 4 * (2 ** 30),
        "user_slots": 1,
    })
    node_memory_limit_addition = attr.ib(0)
    node_chunk_store_quota = attr.ib(None)
    allow_chunk_storage_in_tmpfs = attr.ib(False)

    """Feature flags"""
    enable_master_cache = attr.ib(None)
    enable_permission_cache = attr.ib(None)
    enable_rpc_driver_proxy_discovery = attr.ib(False)
    enable_log_compression = attr.ib(False)
    enable_debug_logging = attr.ib(True)
    enable_structured_logging = attr.ib(False)

    """Port settings"""
    http_proxy_ports = attr.ib(factory=list)
    rpc_proxy_ports = attr.ib(factory=list)

    port_locks_path = attr.ib(None)
    local_port_range = attr.ib(None)
    port_range_start = attr.ib(None)
    node_port_set_size = attr.ib(None)
    listen_port_pool = attr.ib(None)

    """Cluster shape"""
    master_count = attr.ib(1)
    nonvoting_master_count = attr.ib(0)
    clock_count = attr.ib(0)
    secondary_cell_count = attr.ib(0)
    scheduler_count = attr.ib(1)
    controller_agent_count = attr.ib(1)
    node_count = attr.ib(1)
    http_proxy_count = attr.ib(1)
    rpc_proxy_count = attr.ib(1)
    remote_cluster_count = attr.ib(0)

    """Start options"""
    delay_node_start = attr.ib(False)
    delay_scheduler_start = attr.ib(False)

    """Config patches"""
    delta_master_config = attr.ib(None)
    delta_clock_config = attr.ib(None)
    delta_scheduler_config = attr.ib(None)
    delta_controller_agent_config = attr.ib(None)
    delta_node_config = attr.ib(None)
    delta_http_proxy_config = attr.ib(None)
    delta_rpc_proxy_config = attr.ib(None)
    delta_driver_config = attr.ib(None)


__all__ = ["LocalYtConfig"]
