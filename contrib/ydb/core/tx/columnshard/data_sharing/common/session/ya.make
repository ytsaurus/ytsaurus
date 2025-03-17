LIBRARY()

SRCS(
    common.cpp
)

PEERDIR(
    contrib/ydb/library/conclusion
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tx/columnshard/data_sharing/common/context
    contrib/ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
