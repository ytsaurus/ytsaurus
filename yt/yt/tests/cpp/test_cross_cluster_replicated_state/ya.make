GTEST(cpp-integration-test-cross-cluster-replicated-state)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_cross_cluster_replicated_state.cpp
)
DATA(arcadia/yt/yt/tests/cpp/test_cross_cluster_replicated_state/config.yson)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/cpp/test_base
    yt/yt/ytlib
    yt/yt/core/test_framework
    yt/yt/library/named_value
    yt/yt/server/lib/cross_cluster_replicated_state
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


IF (CLANG_COVERAGE)
    REQUIREMENTS(
        ram_disk:16
        ram:32
    )
ELSE()
    REQUIREMENTS(ram_disk:10)
ENDIF()

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:46
        ram:56
    )
ELSE()
    REQUIREMENTS(
        cpu:22
        ram:18
    )
ENDIF()

END()
