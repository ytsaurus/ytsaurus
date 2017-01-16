################################################################################
IF(YMAKE)
################################################################################

SET(YT_ABI 19_1)

LIBRARY()

OWNER(sandello)

NO_COMPILER_WARNINGS()
NO_WERROR()

PYTHON_ADDINCL()

SRCDIR(
  yt/${YT_ABI}/yt/python
)

SRCS(
  yson.cpp
)

PEERDIR(
  contrib/libs/pycxx
  yt/${YT_ABI}/yt/core
  yt/${YT_ABI}/yt/ytlib
  yt/${YT_ABI}/yt/python/common
)

PY_REGISTER(yson_lib)

END()

################################################################################
ENDIF()
################################################################################
