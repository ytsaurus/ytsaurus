LIBRARY()

SRCS(
    operation_id.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/operation_id/protos
    library/cpp/cgiparam
    library/cpp/uri
)

END()
