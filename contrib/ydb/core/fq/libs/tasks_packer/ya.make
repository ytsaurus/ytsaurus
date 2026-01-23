LIBRARY()

SRCS(
    tasks_packer.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/proto
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
