#:yt-updatabale
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc )
set( SRCS
  ${BASE}/channel.cpp
  ${BASE}/channel_cache.cpp
  ${BASE}/client.cpp
  ${BASE}/common.cpp
  ${BASE}/message.cpp
  ${BASE}/retry.cpp
  ${BASE}/server.cpp
  ${BASE}/server_detail.cpp
  ${BASE}/service.cpp
)
set( HDRS
  ${BASE}/channel.h
  ${BASE}/channel_cache.h
  ${BASE}/client.h
  ${BASE}/common.h
  ${BASE}/error.h
  ${BASE}/message.h
  ${BASE}/retry.h
  ${BASE}/server.h
  ${BASE}/server_detail.h
  ${BASE}/service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/rpc.proto YT_SOURCE_FILES )
