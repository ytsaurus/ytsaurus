LIBRARY()

SRCS(
    destination.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/initiator/controller
    contrib/ydb/core/tx/columnshard/data_sharing/common/session
    contrib/ydb/core/tx/columnshard/tablet
    contrib/ydb/core/tx/columnshard/data_sharing/destination/transactions
)

END()
