#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/orchid_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/orchid_service.h
  ${CMAKE_CURRENT_LIST_DIR}/orchid_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/orchid_service.proto YT_SOURCE_FILES )
