LIBRARY()

SRCS(
    file_tree_builder.cpp
    path_list_reader.cpp
)

PEERDIR(
    yql/essentials/providers/common/provider
    contrib/ydb/library/yql/providers/s3/proto
    yql/essentials/utils
    library/cpp/protobuf/util
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
