PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    delivery_validation.cpp
    main.cpp
)

PEERDIR(
    yt/yt/flow/extensions/ydb/cpp
    yt/yt/flow/extensions/ydb/test/proto
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/random
    yt/yt/flow/library/cpp/parsers
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/runner
)

END()

RECURSE_FOR_TESTS(
    unittests
)
