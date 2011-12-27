#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_client )
set( SRCS
  ${BASE}/client_block_cache.cpp
  ${BASE}/common.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/holder_channel_cache.cpp
  ${BASE}/reader_thread.cpp
  ${BASE}/remote_reader.cpp
  ${BASE}/remote_writer.cpp
  ${BASE}/sequential_reader.cpp
  ${BASE}/writer_thread.cpp
)
set( HDRS
  ${BASE}/async_reader.h
  ${BASE}/async_writer.h
  ${BASE}/block_cache.h
  ${BASE}/client_block_cache.h
  ${BASE}/common.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer.h
  ${BASE}/format.h
  ${BASE}/holder_channel_cache.h
  ${BASE}/reader_thread.h
  ${BASE}/remote_reader.h
  ${BASE}/remote_writer.h
  ${BASE}/sequential_reader.h
  ${BASE}/writer_thread.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
