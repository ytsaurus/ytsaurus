IF(YMAKE)

SET(CMAKE_CURRENT_LIST_DIR .)
SET(YT_ABI 19_0)

PROGRAM(ytserver-tools)

NO_COMPILER_WARNINGS()
NO_WERROR()

CXXFLAGS(
  -Wno-sign-compare
  -Wno-unused-parameter
  -Wno-unused-value
  -Wno-switch
  -Wno-type-limits
  -DYT_IN_ARCADIA
)

ADDINCL(
  yt/${YT_ABI}/yt
  yt/${YT_ABI}/yt/contrib/tclap
)

SRCS(
  ${CMAKE_CURRENT_LIST_DIR}/main.cpp
)

PEERDIR(
  yt/${YT_ABI}/yt/server
  library/getopt
)

END()

ENDIF()
