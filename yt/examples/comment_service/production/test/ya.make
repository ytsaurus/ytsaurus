PY3TEST()

TEST_SRCS(test_app.py)

PEERDIR(
    yt/examples/comment_service/production/lib
    mapreduce/yt/python
)

DEPENDS(yt/packages/latest)

END()
