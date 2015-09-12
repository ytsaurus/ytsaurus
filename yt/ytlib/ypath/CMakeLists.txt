#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/rich.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/rich.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
