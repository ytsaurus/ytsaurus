LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    resource_base.cpp
    yt_client_factory_detail.cpp
    yt_hedging_client_detail.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    yt/yt/client/cache
    yt/yt/client/hedging
    library/cpp/yt/misc
)

END()

RECURSE_FOR_TESTS(unittests)
