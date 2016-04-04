OWNER(
    exprmntr
    vartyukh
)

PYTEST()

TEST_SRCS(
    test.py
    test_with_config.py
)

TIMEOUT(100)

DEPENDS(mapreduce/yt/python)

TAG(ya:external)

END()
