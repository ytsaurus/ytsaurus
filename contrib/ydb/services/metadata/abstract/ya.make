LIBRARY()

SRCS(
    common.cpp
    decoder.cpp
    events.cpp
    fetcher.cpp
    kqp_common.cpp
    initialization.cpp
    parsing.cpp
    request_features.cpp
)

GENERATE_ENUM_SERIALIZATION(kqp_common.h)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
    yql/essentials/core/expr_nodes
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/resources
)

END()
