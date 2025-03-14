LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    chunk_block_device.cpp
    chunk_handler.cpp
    dynamic_table_block_device.cpp
    file_system_block_device.cpp
    image_reader.cpp
    memory_block_device.cpp
    profiler.cpp
    random_access_file_reader.cpp
    server.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/ytlib
    yt/yt_proto/yt/client
    yt/yt/server/lib/squash_fs
)

END()
