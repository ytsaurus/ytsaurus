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
    contrib/ydb/library/accessor
    library/cpp/actors/core
    contrib/ydb/services/metadata/request
    contrib/ydb/public/api/protos
    contrib/ydb/core/base
    contrib/ydb/library/yql/core/expr_nodes
)

END()
