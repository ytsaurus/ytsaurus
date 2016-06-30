PYTEST()

OWNER(monster)

TEST_SRCS(
    test.py
)

DEPENDS(
    mapreduce/yt/python
    mapreduce/yt/tests/native
)

NO_LINT()

END()
