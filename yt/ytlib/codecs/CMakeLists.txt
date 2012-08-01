#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/codecs )

set( SRCS
  ${BASE}/codec.cpp
  ${BASE}/snappy.cpp
  ${BASE}/zlib.cpp
  ${BASE}/lz.cpp
  ${BASE}/shared_ref_helpers.cpp
)

set( HDRS
  ${BASE}/codec.h
  ${BASE}/zlib.h
  ${BASE}/lz.h
  ${BASE}/source.h
  ${BASE}/shared_ref_helpers.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
