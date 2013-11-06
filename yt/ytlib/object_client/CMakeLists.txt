#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/object_client )
set( SRCS
  ${BASE}/object_service_proxy.cpp
  ${BASE}/public.cpp
  ${BASE}/helpers.cpp
)
set( HDRS
  ${BASE}/master_ypath_proxy.h
  ${BASE}/object_service_proxy-inl.h
  ${BASE}/object_service_proxy.h
  ${BASE}/object_ypath_proxy.h
  ${BASE}/public.h
  ${BASE}/helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/object_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/master_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/object_service.proto YT_SOURCE_FILES )
