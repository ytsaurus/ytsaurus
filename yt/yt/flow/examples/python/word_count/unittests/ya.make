PY3TEST()

NO_CHECK_IMPORTS()

TEST_SRCS(
    conftest.py
    test_word_count_mapper.py
)

PEERDIR(
    yt/yt/flow/library/python/companion/test_harness
    yt/yt/flow/examples/python/word_count
)

SIZE(SMALL)

END()
