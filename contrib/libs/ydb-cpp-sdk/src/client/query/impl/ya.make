LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    exec_query.cpp
    exec_query.h
    client_session.cpp
    session_state_handler.cpp
)

PEERDIR(
    contrib/libs/ydb-cpp-sdk/src/client/types
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/common_client/impl
    contrib/libs/ydb-cpp-sdk/src/client/proto
)

END()
