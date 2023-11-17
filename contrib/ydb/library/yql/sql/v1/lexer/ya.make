LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/parser/proto_ast
    contrib/ydb/library/yql/parser/proto_ast/gen/v1
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_ansi
    contrib/ydb/library/yql/parser/proto_ast/gen/v1_proto
)

SRCS(
    lexer.cpp
)

SUPPRESSIONS(
    tsan.supp
)

END()
