LIBRARY()

SRCS(
    frequency.cpp
)

PEERDIR(
    yql/essentials/core/sql_types
)

RESOURCE(
    yql/essentials/data/language/rules_corr_basic.json rules_corr_basic.json
)

END()

RECURSE_FOR_TESTS(
    ut
)
