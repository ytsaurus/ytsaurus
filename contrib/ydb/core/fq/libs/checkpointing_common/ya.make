LIBRARY()

SRCS(
    defs.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/graph_params/proto
)

GENERATE_ENUM_SERIALIZATION(defs.h)

END()
