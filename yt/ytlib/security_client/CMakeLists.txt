#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/account_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/group_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/user_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/account_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/user_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/group_ypath.proto YT_SOURCE_FILES )
