LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_resource.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/client
    yt/yt/flow/library/cpp/companion/manager

    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/resources
)

END()

RECURSE_FOR_TESTS(
    unittests
)
