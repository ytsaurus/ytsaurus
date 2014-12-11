#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/scheduler )
set( SRCS
  ${BASE}/helpers.cpp
  ${BASE}/scheduler_channel.cpp
  ${BASE}/statistics.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/helpers.h
  ${BASE}/public.h
  ${BASE}/scheduler_channel.h
  ${BASE}/scheduler_service_proxy.h
  ${BASE}/statistics.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/scheduler_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/job.proto YT_SOURCE_FILES )
