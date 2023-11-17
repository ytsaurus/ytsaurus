LIBRARY()

SRCS(
    external_data_source.cpp
    external_source_factory.cpp
    object_storage.cpp
)

PEERDIR(
    library/cpp/scheme
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    contrib/ydb/library/yql/providers/s3/path_generator
    contrib/ydb/public/sdk/cpp/client/ydb_params
    contrib/ydb/public/sdk/cpp/client/ydb_value
)

END()

RECURSE_FOR_TESTS(
    ut
)
