#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/remote_timestamp_provider.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_listener.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_ypath_proxy.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/remote_timestamp_provider.h
  ${CMAKE_CURRENT_LIST_DIR}/timestamp_provider.h
  ${CMAKE_CURRENT_LIST_DIR}/timestamp_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_listener.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/transaction_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/timestamp_service.proto YT_SOURCE_FILES )
