#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/utilex )
set( SRCS
  ${BASE}/random.cpp
  ${BASE}/lzop.cpp
)
set( HDRS
  ${BASE}/random.h
  ${BASE}/lzop.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
