#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/concurrency )
set( SRCS
  ${BASE}/action_queue.cpp
  ${BASE}/async_semaphore.cpp
  ${BASE}/async_stream.cpp
  ${BASE}/coroutine.cpp
  ${BASE}/delayed_executor.cpp
  ${BASE}/ev_scheduler_thread.cpp
  ${BASE}/execution_context.cpp
  ${BASE}/execution_stack.cpp
  ${BASE}/fiber.cpp
  ${BASE}/fls.cpp
  ${BASE}/fork_aware_spinlock.cpp
  ${BASE}/invoker_queue.cpp
  ${BASE}/periodic_executor.cpp
  ${BASE}/private.cpp
  ${BASE}/scheduler.cpp
  ${BASE}/scheduler_thread.cpp
  ${BASE}/single_queue_scheduler_thread.cpp
  ${BASE}/thread.cpp
  ${BASE}/throughput_throttler.cpp
)
set( HDRS
  ${BASE}/action_queue.h
  ${BASE}/async_semaphore.h
  ${BASE}/async_stream.h
  ${BASE}/atomic_flag_spinlock.h
  ${BASE}/coroutine.h
  ${BASE}/counter-inl.h
  ${BASE}/counter.h
  ${BASE}/delayed_executor.h
  ${BASE}/ev_scheduler_thread.h
  ${BASE}/event_count-inl.h
  ${BASE}/event_count.h
  ${BASE}/execution_context.h
  ${BASE}/execution_stack.h
  ${BASE}/fiber.h
  ${BASE}/fls-inl.h
  ${BASE}/fls.h
  ${BASE}/fork_aware_spinlock.h
  ${BASE}/invoker_queue.h
  ${BASE}/nonblocking_queue-inl.h
  ${BASE}/nonblocking_queue.h
  ${BASE}/parallel_awaiter-inl.h
  ${BASE}/parallel_awaiter.h
  ${BASE}/periodic_executor.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/rw_spinlock-inl.h
  ${BASE}/rw_spinlock.h
  ${BASE}/scheduler-inl.h
  ${BASE}/scheduler.h
  ${BASE}/scheduler_thread.h
  ${BASE}/single_queue_scheduler_thread.h
  ${BASE}/thread.h
  ${BASE}/thread_affinity.h
  ${BASE}/throughput_throttler.h
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
