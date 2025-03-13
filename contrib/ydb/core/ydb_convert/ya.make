LIBRARY()

SRCS(
    column_families.cpp
    compression.cpp
    table_settings.cpp
    table_description.cpp
    table_profiles.cpp
    topic_description.cpp
    ydb_convert.cpp
    tx_proxy_status.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/engine
    contrib/ydb/core/formats/arrow/switch
    yql/essentials/core
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/util
    yql/essentials/types/binary_json
    yql/essentials/types/dynumber
    contrib/ydb/library/conclusion
    contrib/ydb/library/mkql_proto/protos
    yql/essentials/minikql/dom
    yql/essentials/public/udf
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
