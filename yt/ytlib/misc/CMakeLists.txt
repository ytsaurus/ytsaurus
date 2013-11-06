#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/misc )
set( SRCS
  ${BASE}/tclap_helpers.cpp
)
set( HDRS
  ${BASE}/tclap_helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
