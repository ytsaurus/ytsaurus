#:yt-updatabale
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/logging )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/log.cpp
  ${BASE}/pattern.cpp
  ${BASE}/writer.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/log.h
  ${BASE}/pattern.h
  ${BASE}/writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
