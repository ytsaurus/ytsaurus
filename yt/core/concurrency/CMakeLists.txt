#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/concurrency )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/action_queue.cpp
  ${BASE}/action_queue_detail.cpp
  ${BASE}/async_semaphore.cpp
  ${BASE}/thread.cpp
  ${BASE}/delayed_executor.cpp
  ${BASE}/periodic_executor.cpp
  ${BASE}/throughput_throttler.cpp
  ${BASE}/async_stream.cpp
  ${BASE}/coroutine.cpp
  ${BASE}/thread.cpp
  ${BASE}/fiber.cpp
  ${BASE}/scheduler.cpp
  ${BASE}/execution_stack.cpp
  ${BASE}/execution_context.cpp
  ${BASE}/fls.cpp
  ${BASE}/fork_aware_spinlock.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/counter.h
  ${BASE}/counter-inl.h
  ${BASE}/async_stream.h
  ${BASE}/action_queue.h
  ${BASE}/action_queue_detail.h
  ${BASE}/execution_stack.h
  ${BASE}/execution_context.h
  ${BASE}/parallel_awaiter-inl.h
  ${BASE}/parallel_awaiter.h
  ${BASE}/async_semaphore.h
  ${BASE}/thread.h
  ${BASE}/delayed_executor.h
  ${BASE}/periodic_executor.h
  ${BASE}/throughput_throttler.h
  ${BASE}/thread_affinity.h
  ${BASE}/event_count.h
  ${BASE}/event_count-inl.h
  ${BASE}/rw_spinlock.h
  ${BASE}/rw_spinlock-inl.h
  ${BASE}/coroutine.h
  ${BASE}/thread.h
  ${BASE}/scheduler.h
  ${BASE}/scheduler-inl.h
  ${BASE}/fiber.h
  ${BASE}/fls.h
  ${BASE}/fls-inl.h
  ${BASE}/fork_aware_spinlock.h
  ${BASE}/nonblocking_queue.h
  ${BASE}/nonblocking_queue-inl.h
  ${BASE}/atomic_flag_spinlock.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

if (NOT WIN32)
  list(APPEND YT_SOURCE_FILES
    ${BASE}/execution_context-supp.S
  )

  set_source_files_properties(
    ${BASE}/execution_context-supp.S
    PROPERTIES LANGUAGE C
  )
endif()
