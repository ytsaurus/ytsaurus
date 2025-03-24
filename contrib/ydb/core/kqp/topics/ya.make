LIBRARY()

SRCS(
    kqp_topics.cpp
    kqp_topics.h
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/tx/scheme_cache
)

YQL_LAST_ABI_VERSION()


END()
