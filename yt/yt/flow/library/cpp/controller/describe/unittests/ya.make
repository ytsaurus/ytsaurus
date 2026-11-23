GTEST(unittester-flow-controller-describe)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SIZE(MEDIUM)

SRCS(
    describe_ut.cpp
)

RESOURCE(
    ${ARCADIA_ROOT}/yt/yt/flow/tests/key_visitor/cpp/pipeline/pipeline.yson key_visitor/cpp/pipeline/pipeline.yson
    ${ARCADIA_ROOT}/yt/yt/flow/tests/key_visitor/cpp/pipeline_external/pipeline.yson key_visitor/cpp/pipeline_external/pipeline.yson
    ${ARCADIA_ROOT}/yt/yt/flow/tests/key_visitor/joiner/pipeline/pipeline.yson key_visitor/joiner/pipeline/pipeline.yson
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/controller/unittests/mock
    yt/yt/flow/library/cpp/controller
    yt/yt/flow/library/cpp/connectors/random
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/common
    yt/yt/library/query/engine
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

END()
