#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/election )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/election_manager.cpp
  ${BASE}/leader_channel.cpp
  ${BASE}/leader_lookup.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/election_manager.h
  ${BASE}/election_manager_proxy.h
  ${BASE}/leader_channel.h
  ${BASE}/leader_lookup.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/election_manager.proto YT_SOURCE_FILES )
