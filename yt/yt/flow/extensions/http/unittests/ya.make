GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    request_executor_ut.cpp
    sink_ut.cpp
    spec_ut.cpp
)

PEERDIR(
    yt/yt/core/http/mock
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/extensions/http
    yt/yt/library/profiling/solomon
)

SIZE(MEDIUM)

END()
