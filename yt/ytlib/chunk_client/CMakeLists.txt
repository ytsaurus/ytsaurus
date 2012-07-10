#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client )
set( SRCS
  ${BASE}/client_block_cache.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/holder_channel_cache.cpp
  ${BASE}/private.cpp
  ${BASE}/remote_reader.cpp
  ${BASE}/remote_writer.cpp
  ${BASE}/sequential_reader.cpp
  ${BASE}/encoding_writer.cpp
)

set( HDRS
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/block_cache.h
  ${BASE}/client_block_cache.h
  ${BASE}/config.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer.h
  ${BASE}/format.h
  ${BASE}/holder_channel_cache.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/remote_reader.h
  ${BASE}/remote_writer.h
  ${BASE}/sequential_reader.h
  ${BASE}/encoding_writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
