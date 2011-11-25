SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/chunk_meta.proto
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/value.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/schema.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/channel_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/channel_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/chunk_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/chunk_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/chunk_set_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/chunk_set_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/table_writer.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/table_reader.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/common.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/reader.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/writer.h 
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/chunk_meta.proto YT_SOURCE_FILES )
