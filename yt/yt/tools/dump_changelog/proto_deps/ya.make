LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
   GLOBAL main.cpp
)

PEERDIR(
    yt/yt/server/lib/tablet_node
)

END()
