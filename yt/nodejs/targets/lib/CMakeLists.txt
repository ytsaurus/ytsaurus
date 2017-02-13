IF(YMAKE)
  SET(YT_ABI 19_2)
ENDIF()

################################################################################
IF(YMAKE)
################################################################################

LIBRARY(ytlib)

OWNER(snowball sandello)

NO_COMPILER_WARNINGS()

SET(NODE_DIR contrib/libs/node_0_8_28)

PEERDIR(
  ${NODE_DIR}/targets/libnode
  yt/${YT_ABI}/yt/build
  yt/${YT_ABI}/yt/ytlib
  library/streams/lz
  library/streams/lzop
  library/string_utils/base64
)

SRCDIR(yt/${YT_ABI}/yt/nodejs)

CFLAGS(-DSTATIC_NODE_EXTENSION)

SRCS(
  src/async_ref_counted.cpp
  src/common.cpp
  src/error.cpp
  src/future.cpp
  src/stream_base.cpp
  src/node.cpp
  src/input_stack.cpp
  src/input_stream.cpp
  src/input_stub.cpp
  src/output_stack.cpp
  src/output_stream.cpp
  src/output_stub.cpp
  src/driver.cpp
  src/stream_stack.cpp
  src/uv_invoker.cpp

  GLOBAL src/ytnode.cpp
)

END()

################################################################################
ENDIF()
################################################################################

