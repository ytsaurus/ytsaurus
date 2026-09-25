LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    companion_client_detail.cpp
    companion_model.cpp
    companion_proxy.cpp
    companion_singleton_state.cpp
    config.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/companion/proto

    yt/yt/library/profiling/solomon

    yt/yt/core/http
    yt/yt/core/https
    yt/yt/core/rpc/grpc

    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
