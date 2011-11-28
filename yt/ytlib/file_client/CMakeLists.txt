SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client/file_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client/file_writer.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/file_client/file_chunk_server_meta.proto YT_SOURCE_FILES )
