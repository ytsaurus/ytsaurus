#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/orchid_service.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/cypress_integration.h
  ${BASE}/orchid_service.h
  ${BASE}/orchid_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/orchid_service.proto YT_SOURCE_FILES )
