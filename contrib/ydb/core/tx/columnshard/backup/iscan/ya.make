LIBRARY()

SRCS(
    iscan.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)