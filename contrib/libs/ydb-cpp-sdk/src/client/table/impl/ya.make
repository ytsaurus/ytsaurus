LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    client_session.cpp
    data_query.cpp
    readers.cpp
    request_migrator.cpp
    table_client.cpp
    transaction.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/public/api/protos
    contrib/libs/ydb-cpp-sdk/src/client/impl/endpoints
    contrib/libs/ydb-cpp-sdk/src/client/impl/session
    contrib/libs/ydb-cpp-sdk/src/client/table/query_stats
    contrib/libs/ydb-cpp-sdk/src/library/string_utils/helpers
)

END()
