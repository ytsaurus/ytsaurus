GO_TEST()

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_tablets.inc)
ENDIF()

GO_TEST_SRCS(
    admin_client_test.go
    client_test.go
    complex_types_test.go
    compression_test.go
    cypress_client_test.go
    cypress_test.go
    error_injection_test.go
    error_test.go
    files_test.go
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
        tracing_test.go
    )
ENDIF()

END()
