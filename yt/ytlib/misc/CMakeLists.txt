#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/tclap_helpers.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/tclap_helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
