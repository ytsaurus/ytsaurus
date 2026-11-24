PY3TEST()

TEST_SRCS(
    test_codecs.py
    test_read.py
)

PEERDIR(
    yt/yt/flow/extensions/ydb/python
)

END()
