GO_PROGRAM(access_log_viewer)

SRCS(
    main.go
    args.go
    data_model.go
    export_mapper.go
    export_output.go
    export_request.go
    export_tables.go
    exporter.go
    query_builder.go
)

GO_TEST_SRCS(
    export_test.go
    main_test.go
)

IF (OPENSOURCE)
    GO_TEST_SRCS(
        auth_external_test.go
    )

    SRCS(
        args_external.go
        auth_external.go
        http_external.go
    )
ELSE()
    GO_TEST_SRCS(auth_internal_test.go)
    SRCS(
        args_internal.go
        auth_internal.go
        http_internal.go
    )
ENDIF()

END()

RECURSE(
    docs
)

RECURSE_FOR_TESTS(
    tests
)
