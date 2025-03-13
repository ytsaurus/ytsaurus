LIBRARY()

SRCS(
    source.cpp
    cursor.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/common/session
    contrib/ydb/core/tx/columnshard/data_sharing/destination/events
    contrib/ydb/core/tx/columnshard/data_sharing/source/transactions
)

END()
