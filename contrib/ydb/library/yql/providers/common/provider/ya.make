LIBRARY()

SRCS(
    yql_data_provider_impl.cpp
    yql_data_provider_impl.h
    yql_provider.cpp
    yql_provider.h
    yql_provider_names.h
    yql_table_lookup.cpp
    yql_table_lookup.h
)

PEERDIR(
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/sql # fixme
    contrib/ydb/library/yql/core
)

YQL_LAST_ABI_VERSION()

END()
