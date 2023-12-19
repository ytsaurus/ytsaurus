GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    client.go
    client_options.go
    crypto.go
    debug.go
    io.go
    job.go
    job_state.go
    main.go
    mapreduce.go
    operation.go
    operation_options.go
    prepare.go
    reader.go
    reduce_reader.go
    registry.go
    tableswitch.go
    writer.go
)

IF (OPENSOURCE)
    SRCS(
        env.go
    )

    DATA(
        arcadia/yt/go/mapreduce/io_other.go
    )
ENDIF()

IF (NOT OPENSOURCE)
    SRCS(
        env_internal.go
    )
ENDIF()

GO_TEST_SRCS(
    io_test.go
    main_test.go
    reader_test.go
    reduce_reader_test.go
)

GO_XTEST_SRCS(mapreduce_test.go)

IF (OS_LINUX)
    SRCS(io_linux.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(io_other.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(io_other.go)
ENDIF()

END()

RECURSE(
    gotest
    spec
)

IF (NOT OPENSOURCE)
    RECURSE(integration)
ENDIF()
