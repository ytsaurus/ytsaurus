#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/tracing )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/trace_context.cpp
  ${BASE}/trace_manager.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/trace_context-inl.h
  ${BASE}/trace_context.h
  ${BASE}/trace_manager.h
  ${BASE}/trace_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/trace_service.proto YT_SOURCE_FILES )

