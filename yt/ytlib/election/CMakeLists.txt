#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/election )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/cell_manager.cpp
  ${BASE}/election_service_proxy.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/cell_manager.h
  ${BASE}/config.h
  ${BASE}/election_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/election_service.proto YT_SOURCE_FILES )
