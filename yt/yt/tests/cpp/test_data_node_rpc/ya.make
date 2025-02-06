GTEST(cpp-integration-test-data-node-rpc)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_columnar_statistics.cpp
)

EXPLICIT_DATA()
DATA(arcadia/yt/yt/tests/cpp/test_data_node_rpc/config.yson)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

TAG(ya:yt ya:fat ya:huge_logs)

SIZE(LARGE)

REQUIREMENTS(ram:20)

END()
