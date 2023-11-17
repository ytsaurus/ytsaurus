LIBRARY()

SRCS(
    yql_mounts.h
    yql_mounts.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/ydb/library/yql/core/user_data
    contrib/ydb/library/yql/core
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    contrib/ydb/library/yql/mount/lib/yql/aggregate.yql /lib/yql/aggregate.yql
    contrib/ydb/library/yql/mount/lib/yql/window.yql /lib/yql/window.yql
    contrib/ydb/library/yql/mount/lib/yql/id.yql /lib/yql/id.yql
    contrib/ydb/library/yql/mount/lib/yql/sqr.yql /lib/yql/sqr.yql
    contrib/ydb/library/yql/mount/lib/yql/core.yql /lib/yql/core.yql
)

END()
