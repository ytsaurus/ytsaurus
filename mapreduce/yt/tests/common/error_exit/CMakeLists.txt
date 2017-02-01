OWNER(
    ermolovd
    monster
)

PYTEST()

TEST_SRCS(test_error_exit.py)

DATA(
    arcadia/robot/jupiter/junk
)

PEERDIR(
    mapreduce/yt/python
)

DEPENDS(
    yt/packages
    mapreduce/yt/tests/common/error_exit/test_program
)

NO_LINT()

END()
