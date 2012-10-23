#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/rpc )
set( SRCS
  ${BASE}/bus_channel.cpp
  ${BASE}/channel_cache.cpp
  ${BASE}/client.cpp
  ${BASE}/error.cpp
  ${BASE}/message.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/redirector_service.cpp
  ${BASE}/retrying_channel.cpp
  ${BASE}/roaming_channel.cpp
  ${BASE}/dispatcher.cpp
  ${BASE}/scoped_channel.cpp
  ${BASE}/serialized_channel.cpp
  ${BASE}/server.cpp
  ${BASE}/server_detail.cpp
  ${BASE}/service.cpp
)
set( HDRS
  ${BASE}/bus_channel.h
  ${BASE}/channel.h
  ${BASE}/channel_cache.h
  ${BASE}/client.h
  ${BASE}/error.h
  ${BASE}/message.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/redirector_service.h
  ${BASE}/retrying_channel.h
  ${BASE}/roaming_channel.h
  ${BASE}/dispatcher.h
  ${BASE}/scoped_channel.h
  ${BASE}/serialized_channel.h
  ${BASE}/server.h
  ${BASE}/server_detail.h
  ${BASE}/service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/rpc.proto YT_SOURCE_FILES )
