LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/yql/providers/dq/actors
    contrib/ydb/library/yql/providers/dq/actors/events
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    library/cpp/svnversion
    yql/essentials/utils/log
)

SRCS(
    dummy_lock.cpp
    dynamic_nameserver.cpp
)

END()
