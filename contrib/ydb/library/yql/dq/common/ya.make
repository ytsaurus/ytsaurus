LIBRARY()

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/util
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/yql/dq/proto
    contrib/ydb/library/yql/utils
)

SRCS(
    dq_common.cpp
    dq_resource_quoter.h
    dq_value.cpp
    dq_serialized_batch.cpp
)

GENERATE_ENUM_SERIALIZATION(dq_common.h)

END()
