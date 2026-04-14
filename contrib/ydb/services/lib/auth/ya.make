LIBRARY()

SRCS(
    auth_helpers.cpp
)

PEERDIR(
    contrib/ydb/library/aclib
    contrib/ydb/core/base
    contrib/ydb/core/tx/scheme_cache
)

END()
