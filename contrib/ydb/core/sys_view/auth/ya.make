LIBRARY()

SRCS(
    group_members.cpp
    group_members.h
    groups.cpp
    groups.h
    owners.cpp
    owners.h
    permissions.cpp
    permissions.h
    users.cpp
    users.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/kqp/runtime
    contrib/ydb/core/sys_view/common
)

YQL_LAST_ABI_VERSION()

END()
