PROGRAM(file_resource_integration_test)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    file_resource_test.cpp
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/resources/file
    yt/yt/flow/library/cpp/runner
)

END()

RECURSE_FOR_TESTS(test)
