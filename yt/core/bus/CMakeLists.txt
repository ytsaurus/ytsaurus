#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/bus )
set( SRCS
  ${BASE}/config.cpp
  ${BASE}/packet.cpp
  ${BASE}/private.cpp
  ${BASE}/tcp_client.cpp
  ${BASE}/tcp_connection.cpp
  ${BASE}/tcp_dispatcher.cpp
  ${BASE}/tcp_dispatcher_impl.cpp
  ${BASE}/tcp_server.cpp
)
set( HDRS
  ${BASE}/bus.h
  ${BASE}/client.h
  ${BASE}/config.h
  ${BASE}/packet.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/server.h
  ${BASE}/tcp_client.h
  ${BASE}/tcp_connection.h
  ${BASE}/tcp_dispatcher.h
  ${BASE}/tcp_dispatcher_impl.h
  ${BASE}/tcp_server.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
