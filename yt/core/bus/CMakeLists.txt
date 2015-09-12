#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/packet.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tcp_client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tcp_connection.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tcp_dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tcp_dispatcher_impl.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tcp_server.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/bus.h
  ${CMAKE_CURRENT_LIST_DIR}/client.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/packet.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/server.h
  ${CMAKE_CURRENT_LIST_DIR}/tcp_client.h
  ${CMAKE_CURRENT_LIST_DIR}/tcp_connection.h
  ${CMAKE_CURRENT_LIST_DIR}/tcp_dispatcher.h
  ${CMAKE_CURRENT_LIST_DIR}/tcp_dispatcher_impl.h
  ${CMAKE_CURRENT_LIST_DIR}/tcp_server.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
