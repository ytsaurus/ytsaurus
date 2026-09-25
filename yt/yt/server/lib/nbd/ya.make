INCLUDE(${ARCADIA_ROOT}/yt/ya_features.make.inc)

LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    block_device_detail.cpp
    helpers.cpp
    profiler.cpp
    server.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/library/nbd
    yt/yt/core
    yt/yt/ytlib
    yt/yt_proto/yt/client
)

END()

RECURSE(
    chunk
    dynamic_table
    image
    memory
)

IF (YT_ENABLE_JOURNAL_NBD)
    RECURSE(journal)
ENDIF()
