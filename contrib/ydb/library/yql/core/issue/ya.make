LIBRARY()

SRCS(
    yql_issue.cpp
)

PEERDIR(
    library/cpp/resource
    contrib/libs/protobuf
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/core/issue/protos
)

RESOURCE(
    contrib/ydb/library/yql/core/issue/yql_issue.txt yql_issue.txt
)

END()

RECURSE_FOR_TESTS(
    ut
)
