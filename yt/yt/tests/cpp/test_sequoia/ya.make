GTEST(cpp-integration-test-sequoia)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_sequoia.cpp
)
DATA(arcadia/yt/yt/tests/cpp/test_sequoia/config.yson)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

TAG(
    ya:fat
    ya:full_logs
    ya:sys_info
    ya:force_sandbox
    ya:sandbox_coverage
    ya:noretries
    ya:yt
    ya:relwithdebinfo
    ya:dump_test_env
    ya:large_tests_on_single_slots
)


REQUIREMENTS(
    yav:YT_TOKEN=value:sec-01gg4hcd881t1nncr04nmbeg04:yt_token
    yav:ARC_TOKEN=value:sec-01gg4hcd881t1nncr04nmbeg04:arc_token
)

SET(NO_STRIP yes)

SIZE(LARGE)

YT_SPEC(yt/yt/tests/integration/spec.yson)

REQUIREMENTS(
    ram:20
    cpu:4
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:25)
ENDIF()

END()
