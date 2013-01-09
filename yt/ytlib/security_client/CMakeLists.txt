#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/security_client )
set( SRCS
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/account_ypath_proxy.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/account_ypath.proto YT_SOURCE_FILES )
