#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc_helpers.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/cypress_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/lock_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/rpc_helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/rpc.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/cypress_ypath.proto YT_SOURCE_FILES )
