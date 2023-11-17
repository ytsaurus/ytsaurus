LIBRARY()

SRCS(
    file_tree_builder.cpp
    path_list_reader.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/s3/proto
    contrib/ydb/library/yql/utils
    library/cpp/protobuf/util
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
