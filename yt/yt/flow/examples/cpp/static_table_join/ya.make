PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/static_table_join/lib
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/connectors/static_table
    yt/yt/flow/library/cpp/resources
    yt/yt/flow/library/cpp/runner
)

END()

RECURSE_FOR_TESTS(
    test
    unittest
)
