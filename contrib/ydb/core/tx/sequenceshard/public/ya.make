LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
