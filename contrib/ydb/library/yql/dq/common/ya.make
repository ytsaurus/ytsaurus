LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/util
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/yql/dq/proto
    yql/essentials/utils
)

SRCS(
    dq_common.cpp
    dq_resource_quoter.h
    dq_value.cpp
    dq_serialized_batch.cpp
    rope_over_buffer.h
    rope_over_buffer.cpp
)

GENERATE_ENUM_SERIALIZATION(dq_common.h)

END()
