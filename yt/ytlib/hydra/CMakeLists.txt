#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/hydra )
set( SRCS
  ${BASE}/peer_channel.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/version.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/hydra_service_proxy.h
  ${BASE}/peer_channel.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/version.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/hydra_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/hydra_manager.proto YT_SOURCE_FILES )
