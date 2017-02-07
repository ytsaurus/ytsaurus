PYTEST()

OWNER(
    ermolovd
    monster
)

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
    mapreduce/yt/tests/library
    quality/mapreduce
)

TIMEOUT(300)

SIZE(MEDIUM)

NO_LINT()

END()
