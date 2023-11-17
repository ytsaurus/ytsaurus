LIBRARY()

SRCS(
    yql_schema_utils.cpp
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/utils
)

END()

RECURSE(
    expr
    mkql
    parser
    skiff
)
