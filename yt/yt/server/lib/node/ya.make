LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    chunk.cpp
    chunk_location.cpp
    config.cpp
    disk_location.cpp
    private.cpp
)

PEERDIR(
    yt/yt/server/lib/io
    yt/yt/server/lib/misc
    yt/yt/ytlib
    yt/yt/core
    library/cpp/yt/threading
)

END()
