LIBRARY()

SRCS(
    global.cpp
    parse_tree.cpp
    use.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/core
    yql/essentials/parser/antlr_ast/gen/v1_antlr4
    yql/essentials/parser/antlr_ast/gen/v1_ansi_antlr4
)

END()
