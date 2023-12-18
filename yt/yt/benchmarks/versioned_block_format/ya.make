G_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
  main.cpp
)

PEERDIR(
  yt/yt/ytlib
  yt/yt/client
  yt/yt/core

  library/cpp/getopt
)

TAG(ya:not_autocheck)

END()
