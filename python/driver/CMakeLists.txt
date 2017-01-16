################################################################################
IF(YMAKE)
################################################################################

SET(YT_ABI 19_0)

LIBRARY()

OWNER(sandello)

NO_COMPILER_WARNINGS()
NO_WERROR()

PYTHON_ADDINCL()

SRCDIR(
  yt/${YT_ABI}/yt/python
)

SRCS(
  driver.cpp
)

PEERDIR(
  contrib/libs/pycxx
  yt/${YT_ABI}/yt/core
  yt/${YT_ABI}/yt/ytlib
  yt/${YT_ABI}/yt/python/common
)

PY_REGISTER(driver_lib)

END()

################################################################################
ENDIF()
################################################################################
