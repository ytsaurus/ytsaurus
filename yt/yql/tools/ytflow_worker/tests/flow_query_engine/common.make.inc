INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCDIR(yt/yql/tools/ytflow_worker/tests/flow_query_engine)

SRCS(
    group_by_ut.cpp
    message_filter_ut.cpp
    queue_routing_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/queue
)

SIZE(MEDIUM)
