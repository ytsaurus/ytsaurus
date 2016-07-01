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

DEPENDS(mapreduce/yt/python)

TAG(ya:external)

NO_LINT()

END()
