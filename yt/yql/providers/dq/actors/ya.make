YQL_LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    library/cpp/svnversion
    yql/essentials/utils/log
    yt/yql/providers/dq/actors/events
)

SRCS(
    dummy_lock.cpp
    dynamic_nameserver.cpp
)

END()

RECURSE(
    events
    execution
)
