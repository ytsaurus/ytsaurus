#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/misc )
set( SRCS
  ${BASE}/snapshot_builder_detail.cpp
  ${BASE}/build_attributes.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/memory_usage_tracker-inl.h
  ${BASE}/memory_usage_tracker.h
  ${BASE}/public.h
  ${BASE}/snapshot_builder_detail.h
  ${BASE}/build_attributes.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
