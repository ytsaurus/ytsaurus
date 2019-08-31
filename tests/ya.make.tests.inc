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
    test_annotations.py
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
    test_proto_conversion.py
    test_replica_sets.py
    test_resource_cache.py
    test_resources.py
    test_retries.py
    test_scheduler.py
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
)

PEERDIR(
    yp/python

    ${YT_PYTHON_ROOT}/python/yt/environment/arcadia_interop

    contrib/python/flaky
)

REQUIREMENTS(
    # NB: cpu and ram_disk have different meaning for sandbox tasks.
    # 'cpu:n' specifies number of cpus required for <i>one thread</i> of tests.
    # 'ram_disk:m' specifies total volume of ram storage for <i>all test threads</i> (that run in parallel).
    #
    # How we get numbers below?
    # We suppose that 4 GB of disk space is enough for one test.
    # The best host in sandbox has 56 CPU, therefore we get no more than 14 test threads.
    # Each of them requires 4 GB of disk space, so we need 56 GB of ram disk.
    cpu:4
    ram_disk:56
)

SIZE(LARGE)

TAG(ya:fat ya:force_sandbox)

FORK_TEST_FILES()

# Bound the number of parallel tests due to limited space in ram drive.
# !!! This option does not work with FORK_TEST_FILES :-(
# SPLIT_FACTOR(4)

NO_LINT()
