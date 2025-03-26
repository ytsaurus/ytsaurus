GO_TEST()

TAG(ya:huge_logs)

SIZE(MEDIUM)

IF (OPENSOURCE AND AUTOCHECK)
    SKIP_TEST(Tests use docker)
ENDIF()

ENV(YT_STUFF_MAX_START_RETRIES=10)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_tablets.inc)
ENDIF()

GO_TEST_SRCS(
    admin_client_test.go
    auth_client_test.go
    client_test.go
    complex_types_test.go
    compression_test.go
    cypress_client_test.go
    cypress_test.go
    discovery_client_test.go
    error_injection_test.go
    error_test.go
    files_test.go
    goroutine_leakage_test.go
    lock_test.go
    low_level_scheduler_client_test.go
    low_level_tx_client_test.go
    mount_client_test.go
    operations_test.go
    ordered_tables_test.go
    replicated_tables_test.go
    smart_read_test.go
    tables_test.go
    table_backup_test.go
    tablets_test.go
    tls_test.go
    transaction_test.go
)

IF (NOT OPENSOURCE)
    GO_TEST_SRCS(
        main_internal_test.go
        tracing_test.go
    )
ENDIF()

IF (OPENSOURCE)
    GO_TEST_SRCS(
        main_test.go
    )
ENDIF()

END()
