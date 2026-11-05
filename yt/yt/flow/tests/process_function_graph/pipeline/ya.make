PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/tests/process_function_graph/functions
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/runner
)

END()
