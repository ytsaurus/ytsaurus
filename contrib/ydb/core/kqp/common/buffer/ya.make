LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/common/simple
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
