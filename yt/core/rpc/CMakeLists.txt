#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/balancing_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/caching_channel_factory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/channel_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/local_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/local_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/message.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/redirector_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/response_keeper.cpp
  ${CMAKE_CURRENT_LIST_DIR}/retrying_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/roaming_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scoped_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialized_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/server_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/service_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/static_channel_factory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/throttling_channel.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/balancing_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/bus_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/bus_server.h
  ${CMAKE_CURRENT_LIST_DIR}/caching_channel_factory.h
  ${CMAKE_CURRENT_LIST_DIR}/channel.h
  ${CMAKE_CURRENT_LIST_DIR}/channel_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/client.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/dispatcher.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/local_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/local_server.h
  ${CMAKE_CURRENT_LIST_DIR}/message.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/redirector_service.h
  ${CMAKE_CURRENT_LIST_DIR}/response_keeper.h
  ${CMAKE_CURRENT_LIST_DIR}/retrying_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/roaming_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/scoped_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/serialized_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/server.h
  ${CMAKE_CURRENT_LIST_DIR}/server_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/service-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/service.h
  ${CMAKE_CURRENT_LIST_DIR}/service_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/static_channel_factory.h
  ${CMAKE_CURRENT_LIST_DIR}/throttling_channel.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/rpc.proto YT_SOURCE_FILES )
