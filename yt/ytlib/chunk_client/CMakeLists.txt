SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/common.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/format.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/async_writer.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/async_reader.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/remote_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/remote_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/file_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/file_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/sequential_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/holder_channel_cache.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/reader_thread.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/writer_thread.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/retriable_reader.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client/chunk.proto YT_SOURCE_FILES )
