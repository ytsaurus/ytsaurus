#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/erasure )

include_directories(
  ${CMAKE_SOURCE_DIR}/contrib/libs/jerasure
)

set( SRCS
  ${BASE}/public.cpp
  ${BASE}/codec.cpp
  ${BASE}/helpers.cpp
  ${BASE}/jerasure.cpp
  ${BASE}/lrc.cpp
  ${BASE}/reed_solomon.cpp
)

set( HDRS
  ${BASE}/public.h
  ${BASE}/codec.h
  ${BASE}/helpers.h
  ${BASE}/jerasure.h
  ${BASE}/lrc.h
  ${BASE}/reed_solomon.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

