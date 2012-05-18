#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc )
set( SRCS
  ${BASE}/channel.cpp
  ${BASE}/channel_cache.cpp
  ${BASE}/client.cpp
  ${BASE}/common.cpp
  ${BASE}/message.cpp
  ${BASE}/private.cpp
  ${BASE}/redirector_service.cpp
  ${BASE}/retriable_channel.cpp
  ${BASE}/roaming_channel.cpp
  ${BASE}/rpc_manager.cpp
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
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/redirector_service.h
  ${BASE}/retriable_channel.h
  ${BASE}/roaming_channel.h
  ${BASE}/rpc_manager.h
  ${BASE}/server.h
  ${BASE}/server_detail.h
  ${BASE}/service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/rpc.proto YT_SOURCE_FILES )
