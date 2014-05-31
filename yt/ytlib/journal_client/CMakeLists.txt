#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/journal_client )
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
