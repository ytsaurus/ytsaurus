#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/orchid_service.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/orchid_service.h
  ${BASE}/orchid_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/orchid_service.proto YT_SOURCE_FILES )
