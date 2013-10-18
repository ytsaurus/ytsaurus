#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/query_agent )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/query_agent_service.cpp
)

set( HDRS
  ${BASE}/config.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/query_agent_service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

#protoc( ${BASE}/xxx.proto YT_SOURCE_FILES )
