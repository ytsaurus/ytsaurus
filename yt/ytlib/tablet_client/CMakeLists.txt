#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/tablet_client )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/wire_protocol.cpp
  ${BASE}/table_mount_cache.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/wire_protocol.h
  ${BASE}/config.h
  ${BASE}/tablet_service_proxy.h
  ${BASE}/table_mount_cache.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/wire_protocol.proto YT_SOURCE_FILES )
