OWNER(
    ignat
    exprmntr
)

PYTEST()

TEST_SRCS(
    test.py
    test_with_config.py
    test_suspend.py
)

TIMEOUT(100)

SIZE(MEDIUM)

PEERDIR(
    mapreduce/yt/python
)

DEPENDS(yt/packages)

TAG(ya:external)

NO_LINT()

END()
