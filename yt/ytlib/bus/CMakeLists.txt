#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/bus )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/message.cpp
  ${BASE}/packet.cpp
  ${BASE}/tcp_connection.cpp
  ${BASE}/tcp_client.cpp
  ${BASE}/tcp_server.cpp
  ${BASE}/tcp_dispatcher.cpp
  ${BASE}/tcp_dispatcher_impl.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/config.h
  ${BASE}/bus.h
  ${BASE}/client.h
  ${BASE}/message.h
  ${BASE}/packet.h
  ${BASE}/server.h
  ${BASE}/tcp_connection.h
  ${BASE}/tcp_client.h
  ${BASE}/tcp_server.h
  ${BASE}/tcp_dispatcher.h
  ${BASE}/tcp_dispatcher_impl.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
