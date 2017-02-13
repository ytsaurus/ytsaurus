IF(YMAKE)
  SET(YT_ABI 19_2)
ENDIF()

################################################################################
IF(YMAKE)
################################################################################

PROGRAM(ytnode)

OWNER(snowball sandello)

NO_COMPILER_WARNINGS()

SET(NODE_DIR contrib/libs/node_0_8_28)

PEERDIR(
  ${NODE_DIR}/targets/libnode
  yt/${YT_ABI}/yt/nodejs/targets/lib
  library/streams/bzip2
)

SRCS(
  main.cc
)

END()

################################################################################
ENDIF()
################################################################################

