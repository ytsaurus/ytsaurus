#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/election )
set( SRCS
  ${BASE}/election_manager.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/election_callbacks_mock.h
  ${BASE}/election_manager.h
  ${BASE}/private.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
