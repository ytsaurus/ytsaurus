LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/library/discovery_client
    yt/yt/ytlib
)

SRCS(
    discovery.cpp
    helpers.cpp
)

END()

RECURSE_FOR_TESTS(
    unittests
)
