#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/ypath )
set( SRCS
  ${BASE}/rich.cpp
  ${BASE}/token.cpp
  ${BASE}/tokenizer.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/rich.h
  ${BASE}/token.h
  ${BASE}/tokenizer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
