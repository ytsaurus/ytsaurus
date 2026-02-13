GO_PROGRAM(access_log_viewer)

SRCS(
    main.go
    args.go
    data_model.go
    query_builder.go
)

IF (OPENSOURCE)
    SRCS(
        args_external.go
        auth_external.go
        http_external.go
    )
ELSE()
    SRCS(
        args_internal.go
        auth_internal.go
        http_internal.go
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    tests
)
