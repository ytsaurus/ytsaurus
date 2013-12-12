set( BASE ${CMAKE_SOURCE_DIR}/library/messagebus )
set( SRCS
  ${BASE}/acceptor.cpp
  ${BASE}/acceptor_status.cpp
  ${BASE}/connection.cpp
  ${BASE}/coreconn.cpp
  ${BASE}/event_loop.cpp
  ${BASE}/futex_like.cpp
  ${BASE}/handler.cpp
  ${BASE}/key_value_printer.cpp
  ${BASE}/local_flags.cpp
  ${BASE}/locator.cpp
  ${BASE}/mb_lwtrace.cpp
  ${BASE}/message_counter.cpp
  ${BASE}/message.cpp
  ${BASE}/message_status_counter.cpp
  ${BASE}/message_status.cpp
  ${BASE}/messqueue.cpp
  ${BASE}/netaddr.cpp
  ${BASE}/network.cpp
  ${BASE}/queue_config.cpp
  ${BASE}/remote_client_connection.cpp
  ${BASE}/remote_client_session.cpp
  ${BASE}/remote_client_session_semaphore.cpp
  ${BASE}/remote_connection.cpp
  ${BASE}/remote_connection_status.cpp
  ${BASE}/remote_server_connection.cpp
  ${BASE}/remote_server_session.cpp
  ${BASE}/remote_server_session_semaphore.cpp
  ${BASE}/session_config.cpp
  ${BASE}/session.cpp
  ${BASE}/session_impl.cpp
  ${BASE}/session_job_count.cpp
  ${BASE}/session_ptr.cpp
  ${BASE}/socket_addr.cpp
  ${BASE}/status.cpp
  ${BASE}/storage.cpp
  ${BASE}/synchandler.cpp
  ${BASE}/text_utils.cpp
  ${BASE}/use_after_free_checker.cpp
  ${BASE}/use_count_checker.cpp

  ${BASE}/actor/executor.cpp
  ${BASE}/actor/thread_extra.cpp
  ${BASE}/actor/what_thread_does.cpp

  ${BASE}/scheduler/scheduler.cpp

  ${BASE}/protobuf/ybusbuf.cpp  
)

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/libs/sparsehash/src
)

add_library( ytext-arcadia-library-messagebus STATIC ${SRCS} )

target_link_libraries( ytext-arcadia-library-messagebus
  ytext-arcadia-util
  ytext-arcadia-library-blockcodecs
  ytext-arcadia-library-lwtrace
)

