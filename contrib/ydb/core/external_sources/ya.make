LIBRARY()

SRCS(
    external_data_source.cpp
    external_source_builder.cpp
    external_source_factory.cpp
    object_storage.cpp
    validation_functions.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/regex/pcre
    library/cpp/scheme
    contrib/ydb/core/base
    contrib/ydb/core/external_sources/object_storage
    contrib/ydb/core/external_sources/object_storage/inference
    contrib/ydb/core/protos
    contrib/ydb/library/actors/http
    contrib/ydb/library/yql/providers/common/db_id_async_resolver
    yql/essentials/providers/common/gateway
    contrib/ydb/library/yql/providers/s3/common
    contrib/ydb/library/yql/providers/s3/object_listers
    contrib/ydb/library/yql/providers/s3/path_generator
    yql/essentials/public/issue
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/client/params
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()

RECURSE_FOR_TESTS(
    ut
)

RECURSE(
    hive_metastore
    object_storage
    s3
)
