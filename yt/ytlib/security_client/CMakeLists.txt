#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/security_client )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/helpers.cpp
)
set( HDRS
  ${BASE}/account_ypath_proxy.h
  ${BASE}/group_ypath_proxy.h
  ${BASE}/public.h
  ${BASE}/helpers.h
  ${BASE}/user_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/account_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/user_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/group_ypath.proto YT_SOURCE_FILES )
