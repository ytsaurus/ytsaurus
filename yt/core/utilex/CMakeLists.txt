#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/random.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lzop.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/random.h
  ${CMAKE_CURRENT_LIST_DIR}/lzop.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
