#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/scheduler )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/redirector_service.cpp
  ${BASE}/scheduler_service.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/redirector_service.h
  ${BASE}/scheduler_service.h
  ${BASE}/scheduler_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tasks.proto YT_SOURCE_FILES )
protoc( ${BASE}/scheduler_service.proto YT_SOURCE_FILES )
