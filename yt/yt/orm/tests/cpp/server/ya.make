GTEST(cpp-integration-test-orm-master)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    test_yt_connector.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/server

    yt/yt/core/test_framework
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_make_integration.inc)

END()
