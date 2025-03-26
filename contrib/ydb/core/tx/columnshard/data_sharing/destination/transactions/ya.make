LIBRARY()

SRCS(
    tx_start_from_initiator.cpp
    tx_data_from_source.cpp
    tx_finish_from_source.cpp
    tx_finish_ack_from_initiator.cpp
)

PEERDIR(
    contrib/ydb/core/tx/tiering
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/tablet
)

END()
