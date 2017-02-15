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
    mapreduce/yt/tests/common/temp_table/test_temp_table
    yt/packages
)

TIMEOUT(60)

SIZE(FAT)

NO_LINT()

END()
