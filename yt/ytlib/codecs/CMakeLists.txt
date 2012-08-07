#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/codecs )

set( SRCS
  ${BASE}/codec.cpp
  ${BASE}/helpers.cpp
  ${BASE}/lz.cpp
  ${BASE}/snappy.cpp
  ${BASE}/zlib.cpp
)

set( HDRS
  ${BASE}/codec.h
  ${BASE}/helpers.h
  ${BASE}/lz.h
  ${BASE}/public.h
  ${BASE}/snappy.h
  ${BASE}/zlib.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
