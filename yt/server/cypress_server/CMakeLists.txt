#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/access_tracker.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cypress_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cypress_traversing.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lock.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lock_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_proxy_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/virtual.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/access_tracker.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/cypress_traversing.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/lock.h
  ${CMAKE_CURRENT_LIST_DIR}/lock_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/node.h
  ${CMAKE_CURRENT_LIST_DIR}/node_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/node_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/node_proxy_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/type_handler.h
  ${CMAKE_CURRENT_LIST_DIR}/virtual-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/virtual.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/cypress_manager.proto YT_SOURCE_FILES )
