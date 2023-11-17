LIBRARY()

SRCS(
    yql_mkql_file_input_state.cpp
    yql_mkql_file_list.cpp
    yql_mkql_input_stream.cpp
    yql_mkql_input.cpp
    yql_mkql_output.cpp
    yql_mkql_table_content.cpp
    yql_mkql_table.cpp
    yql_mkql_ungrouping_list.cpp
)

PEERDIR(
    library/cpp/streams/brotli
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/providers/yt/codec
    contrib/ydb/library/yql/providers/yt/expr_nodes
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    dq
)

RECURSE_FOR_TESTS(
    ut
)
