LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    tailer.cpp
)

PEERDIR(
    yt/yt/core
    contrib/libs/zstd
)

END()
