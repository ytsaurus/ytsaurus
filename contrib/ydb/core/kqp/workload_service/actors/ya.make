LIBRARY()

SRCS(
    cpu_load_actors.cpp
    pool_handlers_actors.cpp
    scheme_actors.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/workload_service/common
    contrib/ydb/core/kqp/workload_service/tables

    contrib/ydb/core/tx/tx_proxy
)

YQL_LAST_ABI_VERSION()

END()
