LIBRARY()

SRCS(
    yql_files_box.cpp
    yql_files_box.h
    yql_outproc_udf_resolver.cpp
    yql_outproc_udf_resolver.h
    yql_simple_udf_resolver.cpp
    yql_simple_udf_resolver.h
    yql_udf_resolver_with_index.cpp
    yql_udf_resolver_with_index.h
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/protobuf/util
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/schema/expr
)

YQL_LAST_ABI_VERSION()

END()
