LIBRARY()

SRCS(
    row_dispatcher_settings.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/ydb
    contrib/ydb/core/protos
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(row_dispatcher_settings.h)

END()
