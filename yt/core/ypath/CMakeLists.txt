#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/token.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tokenizer.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/token.h
  ${CMAKE_CURRENT_LIST_DIR}/tokenizer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
