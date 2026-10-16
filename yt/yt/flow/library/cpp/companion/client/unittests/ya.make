GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_client_ut.cpp
    companion_model_ut.cpp
    companion_proxy_ut.cpp
    config_ut.cpp
    state_codec_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/companion/client
    yt/yt/library/profiling/solomon
    yt/yt/library/query/engine
)

SIZE(SMALL)

END()
