#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/election )
set( SRCS
  ${BASE}/cell_manager.cpp
  ${BASE}/election_service_proxy.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/config.cpp
)
set( HDRS
  ${BASE}/cell_manager.h
  ${BASE}/config.h
  ${BASE}/election_service_mock.h
  ${BASE}/election_service_proxy.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/election_service.proto YT_SOURCE_FILES )
