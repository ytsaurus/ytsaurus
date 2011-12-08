#:yt-updatabale
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/bus )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/message.cpp
  ${BASE}/message_rearranger.cpp
  ${BASE}/nl_client.cpp
  ${BASE}/nl_server.cpp
  ${BASE}/packet.cpp
)
set( HDRS
  ${BASE}/bus.h
  ${BASE}/client.h
  ${BASE}/common.h
  ${BASE}/message.h
  ${BASE}/message_rearranger.h
  ${BASE}/nl_client.h
  ${BASE}/nl_server.h
  ${BASE}/packet.h
  ${BASE}/server.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
