LIBRARY()

SRCS(
    stages.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/tx_reader
    contrib/ydb/services/metadata/abstract
    contrib/ydb/core/tx/columnshard/blobs_action/events
    contrib/ydb/core/tx/columnshard/data_sharing
)

END()
