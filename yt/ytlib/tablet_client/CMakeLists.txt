#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/tablet_client )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/table_mount_cache.cpp
  ${BASE}/wire_protocol.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/table_mount_cache.h
  ${BASE}/tablet_service_proxy.h
  ${BASE}/wire_protocol.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/wire_protocol.proto YT_SOURCE_FILES )
