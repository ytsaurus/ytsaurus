#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/api )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/client.cpp
  ${BASE}/connection.cpp
  ${BASE}/transaction.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/config.h
  ${BASE}/client.h
  ${BASE}/transaction.h
  ${BASE}/connection.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
