LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    file_system_block_device.cpp
    image_block_device.cpp
    image_reader.cpp
    random_access_file_reader.cpp
    config.cpp
)

PEERDIR(
    yt/yt/server/lib/nbd
    yt/yt/server/lib/squash_fs
    yt/yt/ytlib
    yt/yt/core
)

END()
