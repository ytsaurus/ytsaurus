LIBRARY()

SRCS(
    dnsresolver.cpp
    dnsresolver_caching.cpp
    dnsresolver_ondemand.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/libs/c-ares
)

END()

RECURSE_FOR_TESTS(
    ut
)
