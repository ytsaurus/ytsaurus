#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/compression )

set( SRCS
  ${BASE}/codec.cpp
  ${BASE}/helpers.cpp
  ${BASE}/details.cpp
  ${BASE}/lz.cpp
  ${BASE}/public.cpp
  ${BASE}/snappy.cpp
  ${BASE}/zlib.cpp
)

set( HDRS
  ${BASE}/codec.h
  ${BASE}/helpers.h
  ${BASE}/details.h
  ${BASE}/lz.h
  ${BASE}/public.h
  ${BASE}/snappy.h
  ${BASE}/zlib.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/helpers.proto YT_SOURCE_FILES )