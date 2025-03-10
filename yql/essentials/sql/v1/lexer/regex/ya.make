LIBRARY()

PEERDIR(
    yql/essentials/parser/lexer_common
    yql/essentials/sql/v1/reflect
)

SRCS(
    lexer.cpp
    regex.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
