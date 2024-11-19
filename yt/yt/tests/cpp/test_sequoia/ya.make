GTEST(cpp-integration-test-sequoia)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_sequoia.cpp
)

EXPLICIT_DATA()
DATA(arcadia/yt/yt/tests/cpp/test_sequoia/config.yson)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

SIZE(MEDIUM)

REQUIREMENTS(
    ram:16
    cpu:4
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:20)
ENDIF()

END()
