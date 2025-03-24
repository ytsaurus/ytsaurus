LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/persqueue/writer
    contrib/ydb/library/aclib
)

SRCS(
    scheme_cache.cpp
)

GENERATE_ENUM_SERIALIZATION(scheme_cache.h)

YQL_LAST_ABI_VERSION()

END()
