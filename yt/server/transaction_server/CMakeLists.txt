#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/timestamp_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/timestamp_proxy_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/timestamp_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/timestamp_proxy_service.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/timestamp_manager.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.proto YT_SOURCE_FILES )
