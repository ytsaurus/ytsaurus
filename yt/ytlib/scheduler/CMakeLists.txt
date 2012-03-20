#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/scheduler )
set( SRCS
  ${BASE}/redirector_service.cpp
)
set( HDRS
  ${BASE}/redirector_service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tasks.proto YT_SOURCE_FILES )
protoc( ${BASE}/scheduler_service.proto YT_SOURCE_FILES )
