#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/tools )

set( SRCS
  ${BASE}/tools.cpp
  ${BASE}/registry.cpp
)

set( HDRS
  ${BASE}/public.h
  ${BASE}/tools.h
  ${BASE}/tools-inl.h
  ${BASE}/registry.h
  ${BASE}/registry-inl.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
