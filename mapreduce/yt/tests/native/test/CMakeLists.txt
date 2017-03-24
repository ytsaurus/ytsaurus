PYTEST()

OWNER(
    ermolovd
    monster
)

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

TIMEOUT(299)

SIZE(MEDIUM)

END()
