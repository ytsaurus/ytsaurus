#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/query_agent )
set( SRCS
  ${BASE}/helpers.cpp
  ${BASE}/private.cpp
  ${BASE}/query_executor.cpp
  ${BASE}/query_service.cpp
)

set( HDRS
  ${BASE}/config.h
  ${BASE}/helpers.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/query_executor.h
  ${BASE}/query_service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

