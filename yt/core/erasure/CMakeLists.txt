#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/erasure )

include_directories(
  ${CMAKE_SOURCE_DIR}/contrib/libs/jerasure
)

set( SRCS
  ${BASE}/codec.cpp
  ${BASE}/helpers.cpp
  ${BASE}/jerasure.cpp
  ${BASE}/lrc.cpp
  ${BASE}/public.cpp
  ${BASE}/reed_solomon.cpp
)

set( HDRS
  ${BASE}/codec.h
  ${BASE}/helpers.h
  ${BASE}/jerasure.h
  ${BASE}/lrc.h
  ${BASE}/public.h
  ${BASE}/reed_solomon.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

