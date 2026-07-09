GTEST(unittester-flow-controller-describe)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SIZE(MEDIUM)

SRCS(
    describe_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/controller/unittests/mock
    yt/yt/flow/library/cpp/controller
    yt/yt/flow/library/cpp/connectors/random
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

IF (NOT OPENSOURCE)
    PEERDIR(
        yt/yt/flow/yandex/library/cpp/internal_urls
    )
ENDIF()

END()
