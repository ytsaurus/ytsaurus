LIBRARY()

SRCS(
    signer.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/hmac
)

END()

RECURSE_FOR_TESTS(
    ut
)
