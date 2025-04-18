LIBRARY()

SRCS(
    name.cpp
    parse.cpp
)

PEERDIR(
    yql/essentials/sql/v1/complete/core
)

END()

RECURSE(
    schema
    service
    static
)
