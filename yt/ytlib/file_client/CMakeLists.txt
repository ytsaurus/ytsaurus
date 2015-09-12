#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/file_chunk_output.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/chunk_meta_extensions.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/file_chunk_output.h
  ${CMAKE_CURRENT_LIST_DIR}/file_chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/file_chunk_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/file_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/file_chunk_meta.proto YT_SOURCE_FILES )

