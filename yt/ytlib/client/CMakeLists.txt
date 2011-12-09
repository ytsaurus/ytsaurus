#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/client )
set( SRCS
  ${BASE}/block_buffer.cpp
  ${BASE}/chunk_reader.cpp
  ${BASE}/client.cpp
  ${BASE}/io.cpp
  ${BASE}/io_channels.cpp
  ${BASE}/types.cpp
  ${BASE}/user_indexes.cpp
)
set( HDRS
  ${BASE}/block_buffer.h
  ${BASE}/chunk_reader.h
  ${BASE}/client.h
  ${BASE}/io.h
  ${BASE}/io_channels.h
  ${BASE}/processor.h
  ${BASE}/types.h
  ${BASE}/user_indexes.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk.proto YT_SOURCE_FILES )
