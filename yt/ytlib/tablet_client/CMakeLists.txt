#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/tablet_client )
set( SRCS
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_service.proto YT_SOURCE_FILES )
