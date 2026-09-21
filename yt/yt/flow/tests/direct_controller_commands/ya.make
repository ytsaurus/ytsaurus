PY3TEST()

# Local YT with native-protocol TLS (as in Managed YTsaurus): the proxies dial every other
# service with TLS required, the Flow controller included, which is what breaks the proxy path
# to the controller that this suite works around.
SET(YT_CONFIG_PATCH {rpc_proxy_count=1;node_count=3;scheduler_count=1;jobs_user_slot_count=3;queue_agent_state_target_version=7;enable_tls=%true;})

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/library/python/integration_test_base/recipe.inc)

TEST_SRCS(
    test_direct_controller_commands.py
)

PEERDIR(
    yt/python/yt/wrapper
    yt/yt/flow/library/python/integration_test_base/yt_sync_preset
)

DEPENDS(
    yt/yt/flow/tests/direct_controller_commands/pipeline
    # Local YT generates its TLS certificates with the openssl binary.
    contrib/libs/openssl/apps
)

DATA(arcadia/yt/yt/flow/tests/direct_controller_commands/pipeline/pipeline.yson)

REQUIREMENTS(
    cpu:4
    ram:32
)

# Three federations on a TLS local YT do not fit the medium budget.
TAG(
    ya:fat
    ya:force_sandbox
    ya:huge_logs
)

SIZE(LARGE)

END()

RECURSE(
    pipeline
)
