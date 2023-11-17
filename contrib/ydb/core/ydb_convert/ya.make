LIBRARY()

SRCS(
    column_families.cpp
    compression.cpp
    table_settings.cpp
    table_description.cpp
    table_profiles.cpp
    ydb_convert.cpp
    tx_proxy_status.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/util
    contrib/ydb/library/binary_json
    contrib/ydb/library/dynumber
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/public/udf
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
