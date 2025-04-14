LIBRARY()

SRCS(
    sql_highlight.cpp
)

PEERDIR(
    yql/essentials/sql/v1/lexer/regex
    yql/essentials/sql/v1/reflect
)

END()

RECURSE_FOR_TESTS(
    ut
)
