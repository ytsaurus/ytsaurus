LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/ytlib/discovery_client
    library/cpp/yt/threading
)

SRCS(
    config.cpp
    discovery_base.cpp
    discovery_v1.cpp
    discovery_v2.cpp
    helpers.cpp
)

END()

RECURSE_FOR_TESTS(
    unittests
)
