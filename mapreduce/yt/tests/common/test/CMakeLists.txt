PYTEST()

OWNER(monster)

TEST_SRCS(
    test.py
)

DATA(
    arcadia/kikimr/yql/ci/stuff
    arcadia/devtools/swag
)

PEERDIR(
    mapreduce/yt/python
)

DEPENDS(
    yt/packages
    mapreduce/yt/tests/common
    quality/mapreduce
)

TIMEOUT(300)

NO_LINT()

END()
