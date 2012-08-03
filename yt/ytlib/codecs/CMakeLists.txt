#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/codecs )

set( SRCS
  ${BASE}/codec.cpp
  ${BASE}/snappy.cpp
  ${BASE}/zlib.cpp
  ${BASE}/lz.cpp
  ${BASE}/helpers.cpp
)

set( HDRS
  ${BASE}/codec.h
  ${BASE}/zlib.h
  ${BASE}/lz.h
  ${BASE}/helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
