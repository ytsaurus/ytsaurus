OWNER(
    g:yp
    ignat
)

SET(YP_TEST_SRCDIR yp/tests)

SET(YP_TEST_SRCS
    __init__.py
    conftest.py
    templates.py
    test_accounts.py
    test_acls.py
    test_admin_cli.py
    test_aggregate_objects.py
    test_annotations.py
    test_antiaffinity.py
    test_auth.py
    test_batch_methods.py
    test_cli.py
    test_client_config.py
    test_client_discovery.py
    test_config.py
    test_disk_bandwidth_resource.py
    test_dns_record_set.py
    test_dynamic_resource.py
    test_endpoint_sets.py
    test_endpoints.py
    test_eviction.py
    test_grpc_stubs.py
    test_heavy_scheduler.py
    test_history_api.py
    test_http_api.py
    test_ids.py
    test_internet_address.py
    test_ip4_address_pool.py
    test_labels.py
    test_local_cli.py
    test_multi_cluster_replica_sets.py
    test_net.py
    test_network_projects.py
    test_node_segments.py
    test_nodes.py
    test_object_service.py
    test_objects.py
    test_pod_disruption_budgets.py
    test_pods.py
    test_projects.py
    test_proto_conversion.py
    test_replica_sets.py
    test_resource_cache.py
    test_resources.py
    test_retries.py
    test_scheduler.py
    test_scheduler_cluster.py
    test_scheduler_node_score.py
    test_schemas.py
    test_scripts.py
    # test_secrets.py # TEMPORARY DISABLE
    test_ssl.py
    test_stages.py
    test_sweep.py
    test_timestamps.py
    test_transactions.py
    test_update_objects.py
    test_users.py
    test_virtual_services.py
    test_watches.py
    test_yson_proto.py
    test_yt_environment.py
)

INCLUDE(../python/ya_programs.make.inc)

DEPENDS(
    ${YT_ROOT}/yt/packages/tests_package
    yp/python/yp/bin/yp_admin_make
    yp/python/yp/bin/yp_local_make
    yp/python/yp/bin/yp_make
    yp/scripts/touch_pod_master_spec_timestamps
    yp/server/heavy_scheduler/bin
)

PEERDIR(
    yp/python
    yp/tests/helpers

    ${YT_PYTHON_ROOT}/python/yt/environment/arcadia_interop

    contrib/python/flaky
)

REQUIREMENTS(
    ram_disk:4
    cpu:4
)

SIZE(MEDIUM)

FORK_TESTS()
SPLIT_FACTOR(100)

NO_LINT()
