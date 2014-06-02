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
  ${BASE}/journal_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/journal_ypath.proto YT_SOURCE_FILES )
