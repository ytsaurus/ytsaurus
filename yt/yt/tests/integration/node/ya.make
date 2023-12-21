PY3_LIBRARY()

TEST_SRCS(
    test_disk_quota.py
    test_dynamic_cpu_reclaim.py
    test_gpu_layers.py
    test_heartbeats_to_ca.py
    test_io_engine.py
    test_node_orchid.py
    test_job_proxy.py
    test_jobs.py
    test_hot_swap.py
    test_locations.py
    test_layers.py
    test_node_discovery_manager.py
    test_node_dynamic_config.py
    test_node_flavors.py
    test_node_job_resource_manager.py
    test_p2p.py
    test_user_job.py
)

END()

RECURSE_FOR_TESTS(
    bin
)
