PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SET(YT_CLUSTER_NAMES first)
SET(YT_RECIPE_BUILD_FROM_SOURCE yes)
SET(YT_CONFIG_PATCH {
    rpc_proxy_count = 2;
    node_count = 3;
    node_config = {
        dynamic_config_manager = {
            update_period = 100;
        };
        tablet_node = {
            resource_limits = {
                slots = 1;
            }
        };
        hydra_manager = {
            recovery_min_log_level = debug;
        };
    };
})
INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

TEST_SRCS(
    test.py
)

PEERDIR(
    yt/python/client_with_rpc
    contrib/python/ipython
    yt/yt/tests/integration/stress/dyn_read_write/lib
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/yt_spec.inc)
SIZE(LARGE)
TIMEOUT(1200)

TAG(
    ya:yt
    ya:fat
    ya:huge_logs
    ya:large_tests_on_single_slots
)

REQUIREMENTS(
    cpu:4
    ram_disk:32
    ram:32
)

END()
