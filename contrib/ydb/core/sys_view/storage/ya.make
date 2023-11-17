LIBRARY()

SRCS(
    groups.h
    groups.cpp
    pdisks.h
    pdisks.cpp
    storage_pools.h
    storage_pools.cpp
    storage_stats.h
    storage_stats.cpp
    vslots.h
    vslots.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()
