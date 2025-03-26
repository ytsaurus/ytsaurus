LIBRARY()

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/conclusion
    yql/essentials/core/issue/protos
    contrib/ydb/public/api/protos
)

SRCS(
    modification_type.cpp
    error_codes.cpp
    signals_flow.cpp
)

GENERATE_ENUM_SERIALIZATION(signals_flow.h)

END()
