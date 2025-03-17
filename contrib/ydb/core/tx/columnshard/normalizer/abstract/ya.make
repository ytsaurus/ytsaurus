LIBRARY()

SRCS(
    abstract.cpp
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/core/tx/columnshard/resource_subscriber
)

END()
