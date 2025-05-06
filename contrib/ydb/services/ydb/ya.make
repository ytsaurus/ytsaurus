LIBRARY()

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_debug.cpp
    ydb_dummy.cpp
    ydb_export.cpp
    ydb_import.cpp
    ydb_logstore.cpp
    ydb_operation.cpp
    ydb_query.cpp
    ydb_scheme.cpp
    ydb_scripting.cpp
    ydb_table.cpp
    ydb_object_storage.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/uri
    contrib/ydb/core/base
    contrib/ydb/core/client
    contrib/ydb/core/formats
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/security
    contrib/ydb/core/grpc_streaming
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/library/aclib
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/api/protos
    yql/essentials/public/types
    contrib/libs/openssl
)

END()

RECURSE_FOR_TESTS(
    backup_ut
    table_split_ut
    ut
)
