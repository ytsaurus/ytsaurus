#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/tracing )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/trace_manager.cpp
  ${BASE}/trace_context.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/config.h
  ${BASE}/trace_manager.h
  ${BASE}/trace_service_proxy.h
  ${BASE}/trace_context.h
  ${BASE}/trace_context-inl.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/trace_service.proto YT_SOURCE_FILES )

