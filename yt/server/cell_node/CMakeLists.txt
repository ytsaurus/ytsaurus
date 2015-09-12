#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/bootstrap.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/bootstrap.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)