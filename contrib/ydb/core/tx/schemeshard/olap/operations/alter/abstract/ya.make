LIBRARY()

SRCS(
    object.cpp
    update.cpp
    converter.cpp
    context.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/scheme
    contrib/ydb/library/accessor
    contrib/ydb/core/protos
    contrib/ydb/library/actors/wilson
    contrib/ydb/library/formats/arrow
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()
