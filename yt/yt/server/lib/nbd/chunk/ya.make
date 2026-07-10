LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    chunk_block_device.cpp
    chunk_handler.cpp
    config.cpp
    page_cache.cpp
)

PEERDIR(
    yt/yt/server/lib/nbd
    yt/yt/ytlib
    yt/yt/core
)

END()
