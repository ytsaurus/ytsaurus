PY3TEST()

PEERDIR(
    yt/yt/tools/sqllogictest_generator/lib
    contrib/python/pytest
)

TEST_SRCS(
    test_sqllogic_processor.py
)

DATA(
    arcadia/yt/yt/tools/sqllogictest_generator/lib/test/testdata
)

END()
