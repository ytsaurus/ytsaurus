#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object_service_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/master_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/object_service_proxy-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/object_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/object_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/object_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/master_ypath.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/object_service.proto YT_SOURCE_FILES )
