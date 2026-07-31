PY3TEST()

STYLE_PYTHON()

TEST_SRCS(
    test_chaos.py
    test_pivots.py
)

PEERDIR(
    yt/yt/flow/tools/reshard_flow_tables/lib
)

END()
