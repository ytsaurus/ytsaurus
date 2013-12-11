#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/rpc )
set( SRCS
  ${BASE}/bus_channel.cpp
  ${BASE}/client.cpp
  ${BASE}/dispatcher.cpp
  ${BASE}/helpers.cpp
  ${BASE}/message.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/redirector_service.cpp
  ${BASE}/retrying_channel.cpp
  ${BASE}/roaming_channel.cpp
  ${BASE}/scoped_channel.cpp
  ${BASE}/serialized_channel.cpp
  ${BASE}/server.cpp
  ${BASE}/server_detail.cpp
  ${BASE}/service.cpp
  ${BASE}/service_detail.cpp
  ${BASE}/throttling_channel.cpp
  ${BASE}/static_channel_factory.cpp
  ${BASE}/caching_channel_factory.cpp
)
set( HDRS
  ${BASE}/bus_channel.h
  ${BASE}/channel.h
  ${BASE}/client.h
  ${BASE}/config.h
  ${BASE}/dispatcher.h
  ${BASE}/helpers.h
  ${BASE}/message.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/redirector_service.h
  ${BASE}/retrying_channel.h
  ${BASE}/roaming_channel.h
  ${BASE}/scoped_channel.h
  ${BASE}/serialized_channel.h
  ${BASE}/server.h
  ${BASE}/server_detail.h
  ${BASE}/service.h
  ${BASE}/service_detail.h
  ${BASE}/throttling_channel.h
  ${BASE}/static_channel_factory.h
  ${BASE}/caching_channel_factory.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/rpc.proto YT_SOURCE_FILES )
