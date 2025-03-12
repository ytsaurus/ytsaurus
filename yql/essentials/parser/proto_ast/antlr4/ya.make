LIBRARY()

PEERDIR(
    yql/essentials/parser/antlr_ast/antlr4
    yql/essentials/parser/proto_ast
    contrib/libs/antlr4_cpp_runtime
)

SRCS(
    proto_ast_antlr4.cpp
)

END()


