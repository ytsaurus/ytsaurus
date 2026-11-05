PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/queue
    yt/yt/flow/library/cpp/multiplexer
    yt/yt/flow/library/cpp/process_function/host
    yt/yt/flow/library/cpp/runner
    yt/yt/flow/library/cpp/resources
)

END()
