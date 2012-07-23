#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress_client )
set( SRCS
  ${BASE}/cypress_ypath_proxy.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/cypress_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/cypress_ypath.proto YT_SOURCE_FILES )
