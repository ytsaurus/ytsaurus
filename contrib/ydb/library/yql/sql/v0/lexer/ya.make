LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/parser/proto_ast
    contrib/ydb/library/yql/parser/proto_ast/gen/v0
)

SRCS(
    lexer.cpp
)

END()
