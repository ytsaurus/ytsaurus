LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    dynamic_table_block_device.cpp
)

PEERDIR(
    yt/yt/server/lib/nbd
    yt/yt/client
)

END()
