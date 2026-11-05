PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/parsers
    yt/yt/flow/library/cpp/runner
    yt/yt/flow/tests/transform_ordered_source/pipeline/proto
)

END()
