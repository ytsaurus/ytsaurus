PYTEST()

OWNER(snowball)

TEST_SRCS(test_ytnode.py)

DATA(arcadia/yt/18_5/yt/nodejs)

DEPENDS(
    yt/18_5/yt/node_modules
    yt/18_5/yt/nodejs/targets/bin
)

TIMEOUT(600)

END()
