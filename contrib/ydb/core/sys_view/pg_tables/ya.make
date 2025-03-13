LIBRARY()

SRCS(
    pg_tables.h
    pg_tables.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()

#RECURSE_FOR_TESTS(
#    ut
#)
