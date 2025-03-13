LIBRARY()

SRCS(
    tier_info.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/tx/tiering/tier
)

END()
