#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/journal_server )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
