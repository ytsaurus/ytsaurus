LIBRARY()

SRCS(
    transfer.cpp
    control.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/source/session
)

END()
