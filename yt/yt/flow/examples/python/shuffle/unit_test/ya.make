PY3TEST()

NO_CHECK_IMPORTS()

TEST_SRCS(
    conftest.py
    test_event_mapper.py
    test_event_reducer.py
)

PEERDIR(
    yt/yt/flow/library/python/companion/test_harness
    yt/yt/flow/examples/python/shuffle
)

SIZE(SMALL)

END()
