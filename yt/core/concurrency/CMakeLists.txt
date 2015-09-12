#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/action_queue.cpp
  ${CMAKE_CURRENT_LIST_DIR}/async_semaphore.cpp
  ${CMAKE_CURRENT_LIST_DIR}/async_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/coroutine.cpp
  ${CMAKE_CURRENT_LIST_DIR}/delayed_executor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ev_scheduler_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/execution_context.cpp
  ${CMAKE_CURRENT_LIST_DIR}/execution_stack.cpp
  ${CMAKE_CURRENT_LIST_DIR}/fiber.cpp
  ${CMAKE_CURRENT_LIST_DIR}/fls.cpp
  ${CMAKE_CURRENT_LIST_DIR}/fork_aware_spinlock.cpp
  ${CMAKE_CURRENT_LIST_DIR}/invoker_queue.cpp
  ${CMAKE_CURRENT_LIST_DIR}/periodic_executor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/single_queue_scheduler_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/throughput_throttler.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/action_queue.h
  ${CMAKE_CURRENT_LIST_DIR}/async_semaphore.h
  ${CMAKE_CURRENT_LIST_DIR}/async_stream.h
  ${CMAKE_CURRENT_LIST_DIR}/atomic_flag_spinlock.h
  ${CMAKE_CURRENT_LIST_DIR}/coroutine.h
  ${CMAKE_CURRENT_LIST_DIR}/counter-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/counter.h
  ${CMAKE_CURRENT_LIST_DIR}/delayed_executor.h
  ${CMAKE_CURRENT_LIST_DIR}/ev_scheduler_thread.h
  ${CMAKE_CURRENT_LIST_DIR}/event_count-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/event_count.h
  ${CMAKE_CURRENT_LIST_DIR}/execution_context.h
  ${CMAKE_CURRENT_LIST_DIR}/execution_stack.h
  ${CMAKE_CURRENT_LIST_DIR}/fiber.h
  ${CMAKE_CURRENT_LIST_DIR}/fls-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/fls.h
  ${CMAKE_CURRENT_LIST_DIR}/fork_aware_spinlock.h
  ${CMAKE_CURRENT_LIST_DIR}/invoker_queue.h
  ${CMAKE_CURRENT_LIST_DIR}/nonblocking_queue-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/nonblocking_queue.h
  ${CMAKE_CURRENT_LIST_DIR}/parallel_awaiter-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/parallel_awaiter.h
  ${CMAKE_CURRENT_LIST_DIR}/periodic_executor.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/rw_spinlock-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/rw_spinlock.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler.h
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_thread.h
  ${CMAKE_CURRENT_LIST_DIR}/single_queue_scheduler_thread.h
  ${CMAKE_CURRENT_LIST_DIR}/thread.h
  ${CMAKE_CURRENT_LIST_DIR}/thread_affinity.h
  ${CMAKE_CURRENT_LIST_DIR}/throughput_throttler.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

if (NOT WIN32)
  list(APPEND YT_SOURCE_FILES
    ${CMAKE_CURRENT_LIST_DIR}/execution_context-supp.S
  )

  set_source_files_properties(
    ${CMAKE_CURRENT_LIST_DIR}/execution_context-supp.S
    PROPERTIES LANGUAGE C
  )
endif()
