PY3TEST()

NO_CHECK_IMPORTS()

TEST_SRCS(
    conftest.py
    test_compaction_mapper.py
)

PEERDIR(
    yt/yt/flow/library/python/companion/test_harness
    yt/yt/flow/examples/python/batch_compaction
)

SIZE(SMALL)

END()
