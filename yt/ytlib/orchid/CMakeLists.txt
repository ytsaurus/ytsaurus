#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid )
set( SRCS
  ${BASE}/orchid_service.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/orchid_service.h
  ${BASE}/orchid_service_proxy.h
  ${BASE}/private.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/orchid_service.proto YT_SOURCE_FILES )
