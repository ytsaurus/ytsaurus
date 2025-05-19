GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    test_multiproxy.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)


PEERDIR(
    yt/yt/client
    yt/yt/core/test_framework
)

SET(YT_RECIPE_BUILD_FROM_SOURCE yes)
SET(YT_CLUSTER_NAMES target,read_access,write_access)
SET(YT_CONFIG_PATCH {rpc_proxy_count=1;scheduler_count=0;rpc_proxy_config={enable_authentication=%true;cypress_token_authenticator={secure=%true}}})
INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

YT_SPEC(yt/yt/tests/integration/spec.yson)

TAG(
    ya:yt
    ya:fat
    ya:huge_logs
    ya:large_tests_on_single_slots
)

YT_SPEC(yt/yt/tests/integration/spec.yson)
SIZE(LARGE)
TIMEOUT(1200)

REQUIREMENTS(
    cpu:4
    ram_disk:32
    ram:32
)

END()
