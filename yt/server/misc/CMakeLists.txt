#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/misc )
set( SRCS
  ${BASE}/snapshot_builder_detail.cpp
)
set( HDRS
  ${BASE}/memory_usage_tracker-inl.h
  ${BASE}/memory_usage_tracker.h
  ${BASE}/snapshot_builder_detail.h
  ${BASE}/public.h
  ${BASE}/config.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
