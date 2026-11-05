PY3TEST()

TEST_SRCS(test_state_unpack.py)

PEERDIR(
    yt/yt/flow/library/python/client/state_unpack
)

DATA(arcadia/yt/yt/flow/tests/data/internal_state_rows.yson)

END()
