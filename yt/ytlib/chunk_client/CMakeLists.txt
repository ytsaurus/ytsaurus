#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client )
set( SRCS
  ${BASE}/block_id.cpp
  ${BASE}/chunk_meta_extensions.cpp
  ${BASE}/client_block_cache.cpp
  ${BASE}/dispatcher.cpp
  ${BASE}/encoding_writer.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/holder_channel_cache.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/remote_reader.cpp
  ${BASE}/remote_writer.cpp
  ${BASE}/sequential_reader.cpp
)
set( HDRS
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/block_cache.h
  ${BASE}/block_id.h
  ${BASE}/chunk_holder_service_proxy.h
  ${BASE}/chunk_list_ypath_proxy.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_ypath_proxy.h
  ${BASE}/client_block_cache.h
  ${BASE}/config.h
  ${BASE}/dispatcher.h
  ${BASE}/encoding_writer.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer.h
  ${BASE}/format.h
  ${BASE}/holder_channel_cache.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/remote_reader.h
  ${BASE}/remote_writer.h
  ${BASE}/sequential_reader.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_holder_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_list_ypath.proto YT_SOURCE_FILES )
