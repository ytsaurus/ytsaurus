IF(YMAKE)
  SET(YT_ABI 19_1)
ENDIF()

################################################################################
IF(YMAKE)
################################################################################

PYTEST()

OWNER(snowball sandello)

TEST_SRCS(test_ytnode.py)

DATA(arcadia/yt/${YT_ABI}/yt/nodejs)

DEPENDS(
  yt/${YT_ABI}/yt/node_modules
  yt/${YT_ABI}/yt/nodejs/targets/bin
)

TIMEOUT(600)

END()

################################################################################
ENDIF()
################################################################################

