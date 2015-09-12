#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/trace_context.cpp
  ${CMAKE_CURRENT_LIST_DIR}/trace_manager.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/trace_context-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/trace_context.h
  ${CMAKE_CURRENT_LIST_DIR}/trace_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/trace_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/trace_service.proto YT_SOURCE_FILES )

