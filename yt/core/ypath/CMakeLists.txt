#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/ypath )
set( SRCS
  ${BASE}/token.cpp
  ${BASE}/tokenizer.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/token.h
  ${BASE}/tokenizer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
