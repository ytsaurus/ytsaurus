#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/election )
set( SRCS
  ${BASE}/cell_manager.cpp
  ${BASE}/common.cpp
  ${BASE}/election_manager.cpp
)
set( HDRS
  ${BASE}/cell_manager.h
  ${BASE}/common.h
  ${BASE}/config.h
  ${BASE}/election_manager.h
  ${BASE}/election_manager_proxy.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/election_manager.proto YT_SOURCE_FILES )
