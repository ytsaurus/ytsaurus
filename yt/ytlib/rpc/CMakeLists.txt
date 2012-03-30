#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc )
set( SRCS
  ${BASE}/channel.cpp
  ${BASE}/channel_cache.cpp
  ${BASE}/client.cpp
  ${BASE}/common.cpp
  ${BASE}/message.cpp
  ${BASE}/redirector_service_base.cpp
  ${BASE}/retriable_channel.cpp
  ${BASE}/rpc_manager.cpp
  ${BASE}/server.cpp
  ${BASE}/server_detail.cpp
  ${BASE}/service.cpp
  ${BASE}/roaming_channel.cpp
)
set( HDRS
  ${BASE}/channel.h
  ${BASE}/channel_cache.h
  ${BASE}/client.h
  ${BASE}/common.h
  ${BASE}/error.h
  ${BASE}/message.h
  ${BASE}/redirector_service_base.h
  ${BASE}/retriable_channel.h
  ${BASE}/rpc_manager.h
  ${BASE}/server.h
  ${BASE}/server_detail.h
  ${BASE}/service.h
  ${BASE}/roaming_channel.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/rpc.proto YT_SOURCE_FILES )
