PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/external_state_join/lib
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/resources
    yt/yt/flow/library/cpp/runner
)

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    test
    unittest
)
