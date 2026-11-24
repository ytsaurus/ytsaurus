LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    operation_id.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/library/operation_id/protos
    library/cpp/cgiparam
    library/cpp/uri
)

END()
