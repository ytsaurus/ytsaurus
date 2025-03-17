LIBRARY()

SRCS(
    general.cpp
    changes.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/blobs_action/counters
    contrib/ydb/core/tx/columnshard/counters/common
)

GENERATE_ENUM_SERIALIZATION(changes.h)

END()
