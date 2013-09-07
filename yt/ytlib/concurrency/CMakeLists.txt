#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/concurrency )
set( SRCS
  ${BASE}/action_queue.cpp
  ${BASE}/action_queue_detail.cpp
  ${BASE}/async_semaphore.cpp
  ${BASE}/thread.cpp
  ${BASE}/delayed_invoker.cpp
  ${BASE}/periodic_invoker.cpp
  ${BASE}/throughput_throttler.cpp
  ${BASE}/async_stream.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/async_stream.h
  ${BASE}/action_queue.h
  ${BASE}/action_queue_detail.h
  ${BASE}/parallel_awaiter-inl.h
  ${BASE}/parallel_awaiter.h
  ${BASE}/parallel_collector-inl.h
  ${BASE}/parallel_collector.h
  ${BASE}/async_semaphore.h
  ${BASE}/thread.h
  ${BASE}/delayed_invoker.h
  ${BASE}/periodic_invoker.h
  ${BASE}/throughput_throttler.h
  ${BASE}/thread_affinity.h
  ${BASE}/event_count.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
