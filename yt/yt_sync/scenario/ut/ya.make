PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_dump_diff.py
    test_dump_spec.py
    test_move.py
    test_registry.py
    test_scenario_base.py
    test_switch_replica.py
)

PEERDIR(
    yt/yt_sync/core
    yt/yt_sync/core/fixtures
    yt/yt_sync/core/test_lib
    yt/yt_sync/scenario
)

END()
