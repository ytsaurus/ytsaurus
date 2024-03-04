GTEST(cpp-integration-test-hydra)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    test_remote_changelog_store.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/tests/cpp/test_base
    yt/yt/server/lib/hydra
    yt/yt/ytlib
    yt/yt/core/test_framework
)

TAG(ya:huge_logs)

# remove after YT-19477
IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

IF (YT_TEAMCITY)
    TAG(ya:yt ya:fat)

    YT_SPEC(yt/yt/tests/integration/spec.yson)

    SIZE(LARGE)
ELSE()
    SIZE(MEDIUM)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/recipe/recipe.inc)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

END()
