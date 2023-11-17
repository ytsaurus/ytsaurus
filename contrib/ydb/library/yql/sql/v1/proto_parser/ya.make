LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/utils

    contrib/ydb/library/yql/parser/proto_ast
    contrib/ydb/library/yql/parser/proto_ast/collect_issues
    contrib/ydb/library/yql/parser/proto_ast/gen/v1
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_ansi
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto
)

SRCS(
    proto_parser.cpp
)

END()
