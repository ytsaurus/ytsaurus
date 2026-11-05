PY3TEST()

# A second cluster is needed for the cross-cluster reanimate test (runtime cluster != pipeline cluster).
SET(YT_CLUSTER_NAMES primary,remote_0)

# Match the local RPC proxy's anonymous test configuration for the in-job RPC proxy.
SET(YT_CONFIG_PATCH {rpc_proxy_count=1;node_count=3;scheduler_count=1;jobs_user_slot_count=3;queue_agent_state_target_version=7;node_config={exec_node={job_proxy={job_proxy_authentication_manager={require_authentication=%false;};};};};})

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_reanimate.py
    yt_sync.py
)

DEPENDS(
    yt/yt/flow/tests/reanimate_vanilla/cpp/pipeline
    yt/yt/flow/tools/reanimate_vanilla_operation
)

DATA(arcadia/yt/yt/flow/tests/reanimate_vanilla/cpp/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

# The sanitized binaries are too slow for this heavyweight integration scenario; the regular
# builds keep the coverage.
IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ENDIF()

TAG(ya:huge_logs)

# Each vanilla test starts its own local YT; keep its time budget independent of the other cases.
FORK_SUBTESTS()
SPLIT_FACTOR(4)

SIZE(MEDIUM)

END()
