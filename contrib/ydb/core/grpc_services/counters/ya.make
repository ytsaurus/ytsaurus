LIBRARY()

SRCS(
    counters.cpp
    counters.h
    proxy_counters.cpp
    proxy_counters.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/sys_view/service
)

YQL_LAST_ABI_VERSION()

END()
