LIBRARY()

SRCS(
    kqp_shards_resolver.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/actorlib_impl
    contrib/ydb/core/base
)

YQL_LAST_ABI_VERSION()

END()
