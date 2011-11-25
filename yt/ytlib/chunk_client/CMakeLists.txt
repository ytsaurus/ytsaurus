SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/common.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/format.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/chunk_writer.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/remote_chunk_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/remote_chunk_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/file_chunk_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/chunk_reader.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/file_chunk_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/holder_channel_cache.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/sequential_chunk_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/reader_thread.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/writer_thread.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/chunk.proto YT_SOURCE_FILES )
