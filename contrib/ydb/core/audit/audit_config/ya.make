LIBRARY()

SRCS(
    audit_config.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/aclib/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
