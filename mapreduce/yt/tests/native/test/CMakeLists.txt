PYTEST()

OWNER(monster)

TEST_SRCS(
    test.py
)

PEERDIR(
    mapreduce/yt/python
)

DEPENDS(
    yt/packages
    mapreduce/yt/tests/native
)

NO_LINT()

END()
