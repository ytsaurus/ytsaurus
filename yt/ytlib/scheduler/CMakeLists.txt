#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/scheduler )
set( SRCS
  ${BASE}/helpers.cpp
  ${BASE}/scheduler_channel.cpp
)
set( HDRS
  ${BASE}/helpers.h
  ${BASE}/public.h
  ${BASE}/config.h
  ${BASE}/scheduler_proxy.h
  ${BASE}/scheduler_channel.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/scheduler_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/job.proto YT_SOURCE_FILES )
