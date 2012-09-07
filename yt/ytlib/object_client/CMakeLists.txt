#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/object_client )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/object_service_proxy.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/object_service_proxy-inl.h
  ${BASE}/object_service_proxy.h
  ${BASE}/object_ypath_proxy.h
  ${BASE}/object_service_proxy-inl.h
  ${BASE}/object_service_proxy.h
  ${BASE}/object_ypath_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/object_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/object_service.proto YT_SOURCE_FILES )
