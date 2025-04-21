UNITTEST_FOR(yql/essentials/sql/v1/complete)

SRCS(
    sql_complete_ut.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/schema/static
    yql/essentials/sql/v1/complete/name/service/fallback
    yql/essentials/sql/v1/complete/name/service/ranking
    yql/essentials/sql/v1/complete/name/service/schema
    yql/essentials/sql/v1/complete/name/service/static
    yql/essentials/sql/v1/complete/name/service/union
    yql/essentials/sql/v1/lexer/antlr4_pure
    yql/essentials/sql/v1/lexer/antlr4_pure_ansi
)

END()
