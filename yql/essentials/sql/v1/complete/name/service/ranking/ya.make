LIBRARY()

SRCS(
    frequency.cpp
    ranking.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/name/service
)

END()

RECURSE_FOR_TESTS(
    ut
)
