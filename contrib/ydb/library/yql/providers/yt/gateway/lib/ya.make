LIBRARY()

SRCS(
    query_cache.cpp
    query_cache.h
    temp_files.cpp
    temp_files.h
    transaction_cache.cpp
    transaction_cache.h
    user_files.cpp
    user_files.h
    yt_helpers.cpp
    yt_helpers.h
)

PEERDIR(
    library/cpp/regex/pcre
    library/cpp/string_utils/url
    library/cpp/threading/future
    library/cpp/yson/node
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/utils/threading
    contrib/ydb/library/yql/core/type_ann
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/gateway
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/yt/lib/hash
    contrib/ydb/library/yql/providers/yt/lib/res_pull
    contrib/ydb/library/yql/providers/yt/lib/url_mapper
    contrib/ydb/library/yql/providers/yt/lib/yson_helpers
)

YQL_LAST_ABI_VERSION()

END()
