GO_LIBRARY()

LICENSE(MIT)

SRCS(
    array.go
    array_go118.go
    config.go
    doc.go
    encoder.go
    error.go
    field.go
    flag.go
    global.go
    http_handler.go
    level.go
    logger.go
    options.go
    sink.go
    stacktrace.go
    sugar.go
    time.go
    writer.go
)

GO_TEST_SRCS(
    array_go118_test.go
    array_test.go
    clock_test.go
    common_test.go
    config_test.go
    encoder_test.go
    error_test.go
    field_test.go
    flag_test.go
    global_test.go
    increase_level_test.go
    leak_test.go
    level_test.go
    logger_bench_test.go
    logger_test.go
    sink_test.go
    stacktrace_test.go
    sugar_test.go
    time_test.go
    writer_test.go
)

GO_XTEST_SRCS(
    example_go118_test.go
    example_test.go
    http_handler_test.go
    # stacktrace_ext_test.go
)

IF (OS_WINDOWS)
    GO_TEST_SRCS(sink_windows_test.go)
ENDIF()

END()

RECURSE(
    #    benchmarks
    buffer
    gotest
    internal
    zapcore
    zapgrpc
    zapio
    zaptest
)
