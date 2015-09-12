#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/attribute_set.cpp
  ${CMAKE_CURRENT_LIST_DIR}/garbage_collector.cpp
  ${CMAKE_CURRENT_LIST_DIR}/master.cpp
  ${CMAKE_CURRENT_LIST_DIR}/master_cache_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schema.cpp
  ${CMAKE_CURRENT_LIST_DIR}/staged_object.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sys_node.cpp
  ${CMAKE_CURRENT_LIST_DIR}/type_handler.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/attribute_set.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/garbage_collector.h
  ${CMAKE_CURRENT_LIST_DIR}/master.h
  ${CMAKE_CURRENT_LIST_DIR}/master_cache_service.h
  ${CMAKE_CURRENT_LIST_DIR}/object.h
  ${CMAKE_CURRENT_LIST_DIR}/object-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/object_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/object_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/object_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/object_service.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/schema.h
  ${CMAKE_CURRENT_LIST_DIR}/staged_object.h
  ${CMAKE_CURRENT_LIST_DIR}/sys_node.h
  ${CMAKE_CURRENT_LIST_DIR}/type_handler.h
  ${CMAKE_CURRENT_LIST_DIR}/type_handler_detail.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/object_manager.proto YT_SOURCE_FILES )
