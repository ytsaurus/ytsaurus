#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/query_agent )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/helpers.cpp
  ${BASE}/query_service.cpp
  ${BASE}/query_executor.cpp
)

set( HDRS
  ${BASE}/config.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/helpers.h
  ${BASE}/query_service.h
  ${BASE}/query_executor.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

