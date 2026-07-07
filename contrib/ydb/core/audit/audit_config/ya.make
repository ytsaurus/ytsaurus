LIBRARY()

SRCS(
    audit_config.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/aclib/protos/identity
)

END()

RECURSE_FOR_TESTS(
    ut
)
