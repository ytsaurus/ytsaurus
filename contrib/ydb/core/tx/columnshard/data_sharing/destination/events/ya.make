LIBRARY()

SRCS(
    transfer.cpp
    status.cpp
    control.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/data_sharing/destination/session
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/library/actors/core
)

END()
