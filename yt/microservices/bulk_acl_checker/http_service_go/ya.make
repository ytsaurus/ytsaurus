GO_PROGRAM(bulk_acl_checker)

SRCS(
    acl_cache.go
    acl_check.go
    main.go
    metrics.go
    run.go
    args.go
)

IF (OPENSOURCE)
    SRCS(
        args_external.go
        run_external.go
    )
ELSE()
    SRCS(
        run_internal.go
        args_internal.go
    )
ENDIF()

PEERDIR(
    ${GOSTD}/log
)

END()

RECURSE_FOR_TESTS(
    tests
)
