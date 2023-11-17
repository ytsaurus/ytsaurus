LIBRARY()

SRCS(
    GLOBAL behaviour.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/gateway/behaviour/tablestore
)

YQL_LAST_ABI_VERSION()

END()
