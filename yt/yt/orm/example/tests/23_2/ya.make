PY3TEST()

INCLUDE(../ya.make.inc)

NO_SANITIZE()

TEST_SRCS(${TEST_FILES})

DEPENDS(yt/packages/23_2)

ENV(YT_TESTS_PACKAGE_DIR=yt/packages/23_2)

END()
