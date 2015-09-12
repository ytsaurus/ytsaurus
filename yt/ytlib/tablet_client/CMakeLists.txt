#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_mount_cache.cpp
  ${CMAKE_CURRENT_LIST_DIR}/wire_protocol.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/table_mount_cache.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/wire_protocol.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/tablet_service.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/wire_protocol.proto YT_SOURCE_FILES )
