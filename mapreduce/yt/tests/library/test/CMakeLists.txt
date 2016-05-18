PYTEST()

OWNER(monster)

TEST_SRCS(
    test.py
)

DATA(
    arcadia/kikimr/ci
    arcadia/kikimr/yql/ci/stuff
    arcadia/devtools/swag
)

DEPENDS(
    mapreduce/yt/python
    mapreduce/yt/tests/library
    quality/mapreduce
)

TIMEOUT(300)

END()
