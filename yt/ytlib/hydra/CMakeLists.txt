#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/peer_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/version.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/hydra_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/peer_channel.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/version.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/hydra_service.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/hydra_manager.proto YT_SOURCE_FILES )
