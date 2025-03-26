LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
)

SRCS(
    balance_coverage_builder.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
