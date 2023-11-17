LIBRARY()

SRCS(
    yql_facade.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/random_provider
    library/cpp/string_utils/base64
    library/cpp/threading/future
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/library/yql/core/extract_predicate
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/services
    contrib/ydb/library/yql/core/url_lister/interface
    contrib/ydb/library/yql/core/url_preprocessing/interface
    contrib/ydb/library/yql/core/credentials
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/providers/common/config
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/common/arrow_resolve
    contrib/ydb/library/yql/providers/config
    contrib/ydb/library/yql/providers/result/provider
)

YQL_LAST_ABI_VERSION()

END()
