LIBRARY()

SRCS(
    sessions.cpp
    shared_blobs.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_sharing/source/session
    contrib/ydb/core/tx/columnshard/data_sharing/destination/session
)

END()
