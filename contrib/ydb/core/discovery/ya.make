LIBRARY()

SRCS(
    discovery.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/tx/scheme_cache
    contrib/ydb/library/actors/core
)

END()
