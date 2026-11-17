YQL_LIBRARY()

SRCS(
    full_result_writer.cpp
    graph_execution_events_actor.cpp
    result_aggregator.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/dq/actors
    library/cpp/threading/future
    library/cpp/yson
    yql/essentials/providers/common/provider
    yql/essentials/sql
    yt/yql/providers/dq/interface
)

END()

RECURSE_FOR_TESTS(
    ut
)
