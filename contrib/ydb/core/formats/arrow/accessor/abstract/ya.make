LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/apache/arrow_next
    contrib/ydb/library/conclusion
    contrib/ydb/services/metadata/abstract
    contrib/ydb/library/actors/core
    contrib/ydb/core/formats/arrow/accessor/common
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/library/arrow_kernels
)

SRCS(
    common.cpp
    constructor.cpp
    request.cpp
    accessor.cpp
    minmax_utils.cpp
)

GENERATE_ENUM_SERIALIZATION(accessor.h)

END()
