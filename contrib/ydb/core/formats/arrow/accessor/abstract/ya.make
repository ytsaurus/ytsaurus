LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/conclusion
    contrib/ydb/services/metadata/abstract
    contrib/ydb/library/actors/core
    contrib/ydb/core/formats/arrow/accessor/common
    contrib/ydb/library/formats/arrow/protos
)

SRCS(
    common.cpp
    constructor.cpp
    request.cpp
    accessor.cpp
)

GENERATE_ENUM_SERIALIZATION(accessor.h)

END()
