LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    row_level_security.cpp
    proto/row_level_security.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/client
)

END()
