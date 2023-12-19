PY3_LIBRARY()

TEST_SRCS(
    test_bootstrap.py
    test_clock_server.py
    test_discovery.py
    test_erasure.py
    test_error_codes.py
    test_file_cache.py
    test_files.py
    test_metrics.py
    test_get_supported_features.py
    test_io_tracking.py
    test_orchid.py
    test_schema.py
    test_schema_unified.py
    test_security_tags.py
    test_skynet.py
    test_snapshot_validation.py
    test_tables.py
    test_timestamp_provider.py
    test_tracing.py
    test_admin_commands_acl.py
)

END()

RECURSE_FOR_TESTS(
    bin
)
