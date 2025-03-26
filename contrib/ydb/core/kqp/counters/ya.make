LIBRARY()

SRCS(
    kqp_counters.cpp
    kqp_counters.h
    kqp_db_counters.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tx/tx_proxy
    contrib/ydb/core/sys_view/service
    contrib/ydb/library/yql/dq/actors/spilling
    yql/essentials/minikql
)

END()
