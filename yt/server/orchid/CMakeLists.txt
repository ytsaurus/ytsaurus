#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/cypress_integration.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
