LIBRARY()

PEERDIR(
    yql/essentials/public/issue
    yql/essentials/parser/lexer_common
    yql/essentials/sql/settings
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
