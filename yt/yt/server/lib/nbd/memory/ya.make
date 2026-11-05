LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    memory_block_device.cpp
)

PEERDIR(
    yt/yt/server/lib/nbd
)

END()
