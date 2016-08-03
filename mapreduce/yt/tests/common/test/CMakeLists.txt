PYTEST()

OWNER(monster)

TEST_SRCS(
    test.py
)

DATA(
    arcadia/kikimr/yql/ci/stuff
    arcadia/devtools/swag
)

DEPENDS(
    mapreduce/yt/python
    mapreduce/yt/tests/common
    quality/mapreduce
)

TIMEOUT(300)

NO_LINT()

END()
