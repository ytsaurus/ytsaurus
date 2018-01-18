set(NAME deps-arcadia-contrib-libs-grpc)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/grpc)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error")

set(SRCS_CORE
  ${BASE}/src/core/ext/census/base_resources.c
  #${BASE}/src/core/ext/census/census_init.c
  #${BASE}/src/core/ext/census/census_log.c
  #${BASE}/src/core/ext/census/census_rpc_stats.c
  #${BASE}/src/core/ext/census/census_tracing.c
  ${BASE}/src/core/ext/census/context.c
  ${BASE}/src/core/ext/census/gen/census.pb.c
  ${BASE}/src/core/ext/census/gen/trace_context.pb.c
  ${BASE}/src/core/ext/census/grpc_context.c
  ${BASE}/src/core/ext/census/grpc_filter.c
  ${BASE}/src/core/ext/census/grpc_plugin.c
  #${BASE}/src/core/ext/census/hash_table.c
  ${BASE}/src/core/ext/census/initialize.c
  ${BASE}/src/core/ext/census/mlog.c
  ${BASE}/src/core/ext/census/operation.c
  ${BASE}/src/core/ext/census/placeholders.c
  ${BASE}/src/core/ext/census/resource.c
  ${BASE}/src/core/ext/census/trace_context.c
  ${BASE}/src/core/ext/census/tracing.c
  #${BASE}/src/core/ext/census/window_stats.c

  ${BASE}/src/core/ext/client_channel/channel_connectivity.c
  ${BASE}/src/core/ext/client_channel/client_channel.c
  ${BASE}/src/core/ext/client_channel/client_channel_factory.c
  ${BASE}/src/core/ext/client_channel/client_channel_plugin.c
  ${BASE}/src/core/ext/client_channel/connector.c
  ${BASE}/src/core/ext/client_channel/default_initial_connect_string.c
  ${BASE}/src/core/ext/client_channel/http_connect_handshaker.c
  ${BASE}/src/core/ext/client_channel/initial_connect_string.c
  ${BASE}/src/core/ext/client_channel/lb_policy.c
  ${BASE}/src/core/ext/client_channel/lb_policy_factory.c
  ${BASE}/src/core/ext/client_channel/lb_policy_registry.c
  ${BASE}/src/core/ext/client_channel/parse_address.c
  ${BASE}/src/core/ext/client_channel/resolver.c
  ${BASE}/src/core/ext/client_channel/resolver_factory.c
  ${BASE}/src/core/ext/client_channel/resolver_registry.c
  ${BASE}/src/core/ext/client_channel/subchannel.c
  ${BASE}/src/core/ext/client_channel/subchannel_index.c
  ${BASE}/src/core/ext/client_channel/uri_parser.c

  ${BASE}/src/core/ext/lb_policy/grpclb/grpclb.c
  ${BASE}/src/core/ext/lb_policy/grpclb/load_balancer_api.c
  ${BASE}/src/core/ext/lb_policy/grpclb/proto/grpc/lb/v1/load_balancer.pb.c
  ${BASE}/src/core/ext/lb_policy/pick_first/pick_first.c
  ${BASE}/src/core/ext/lb_policy/round_robin/round_robin.c

  ${BASE}/src/core/ext/load_reporting/load_reporting.c
  ${BASE}/src/core/ext/load_reporting/load_reporting_filter.c

  ${BASE}/src/core/ext/resolver/dns/native/dns_resolver.c
  ${BASE}/src/core/ext/resolver/sockaddr/sockaddr_resolver.c

  ${BASE}/src/core/ext/transport/chttp2/alpn/alpn.c
  ${BASE}/src/core/ext/transport/chttp2/client/chttp2_connector.c
  ${BASE}/src/core/ext/transport/chttp2/client/insecure/channel_create.c
  ${BASE}/src/core/ext/transport/chttp2/client/insecure/channel_create_posix.c
  ${BASE}/src/core/ext/transport/chttp2/client/secure/secure_channel_create.c
  ${BASE}/src/core/ext/transport/chttp2/server/chttp2_server.c
  ${BASE}/src/core/ext/transport/chttp2/server/insecure/server_chttp2.c
  ${BASE}/src/core/ext/transport/chttp2/server/insecure/server_chttp2_posix.c
  ${BASE}/src/core/ext/transport/chttp2/server/secure/server_secure_chttp2.c
  ${BASE}/src/core/ext/transport/chttp2/transport/bin_decoder.c
  ${BASE}/src/core/ext/transport/chttp2/transport/bin_encoder.c
  ${BASE}/src/core/ext/transport/chttp2/transport/chttp2_plugin.c
  ${BASE}/src/core/ext/transport/chttp2/transport/chttp2_transport.c
  ${BASE}/src/core/ext/transport/chttp2/transport/frame_data.c
  ${BASE}/src/core/ext/transport/chttp2/transport/frame_goaway.c
  ${BASE}/src/core/ext/transport/chttp2/transport/frame_ping.c
  ${BASE}/src/core/ext/transport/chttp2/transport/frame_rst_stream.c
  ${BASE}/src/core/ext/transport/chttp2/transport/frame_settings.c
  ${BASE}/src/core/ext/transport/chttp2/transport/frame_window_update.c
  ${BASE}/src/core/ext/transport/chttp2/transport/hpack_encoder.c
  ${BASE}/src/core/ext/transport/chttp2/transport/hpack_parser.c
  ${BASE}/src/core/ext/transport/chttp2/transport/hpack_table.c
  ${BASE}/src/core/ext/transport/chttp2/transport/huffsyms.c
  ${BASE}/src/core/ext/transport/chttp2/transport/incoming_metadata.c
  ${BASE}/src/core/ext/transport/chttp2/transport/parsing.c
  ${BASE}/src/core/ext/transport/chttp2/transport/status_conversion.c
  ${BASE}/src/core/ext/transport/chttp2/transport/stream_lists.c
  ${BASE}/src/core/ext/transport/chttp2/transport/stream_map.c
  ${BASE}/src/core/ext/transport/chttp2/transport/varint.c
  ${BASE}/src/core/ext/transport/chttp2/transport/writing.c
  #${BASE}/src/core/ext/transport/cronet/client/secure/cronet_channel_create.c
  #${BASE}/src/core/ext/transport/cronet/transport/cronet_api_dummy.c
  #${BASE}/src/core/ext/transport/cronet/transport/cronet_transport.c

  ${BASE}/src/core/lib/channel/channel_args.c
  ${BASE}/src/core/lib/channel/channel_stack_builder.c
  ${BASE}/src/core/lib/channel/channel_stack.c
  ${BASE}/src/core/lib/channel/compress_filter.c
  ${BASE}/src/core/lib/channel/connected_channel.c
  ${BASE}/src/core/lib/channel/deadline_filter.c
  ${BASE}/src/core/lib/channel/handshaker.c
  ${BASE}/src/core/lib/channel/handshaker_factory.c
  ${BASE}/src/core/lib/channel/handshaker_registry.c
  ${BASE}/src/core/lib/channel/http_client_filter.c
  ${BASE}/src/core/lib/channel/http_server_filter.c
  ${BASE}/src/core/lib/channel/message_size_filter.c
  ${BASE}/src/core/lib/compression/compression.c
  ${BASE}/src/core/lib/compression/message_compress.c
  ${BASE}/src/core/lib/debug/trace.c
  ${BASE}/src/core/lib/http/format_request.c
  ${BASE}/src/core/lib/http/httpcli.c
  ${BASE}/src/core/lib/http/httpcli_security_connector.c
  ${BASE}/src/core/lib/http/parser.c
  ${BASE}/src/core/lib/iomgr/closure.c
  ${BASE}/src/core/lib/iomgr/combiner.c
  ${BASE}/src/core/lib/iomgr/endpoint.c
  ${BASE}/src/core/lib/iomgr/endpoint_pair_posix.c
  ${BASE}/src/core/lib/iomgr/endpoint_pair_uv.c
  ${BASE}/src/core/lib/iomgr/endpoint_pair_windows.c
  ${BASE}/src/core/lib/iomgr/error.c
  ${BASE}/src/core/lib/iomgr/ev_epoll_linux.c
  ${BASE}/src/core/lib/iomgr/ev_poll_posix.c
  ${BASE}/src/core/lib/iomgr/ev_posix.c
  ${BASE}/src/core/lib/iomgr/exec_ctx.c
  ${BASE}/src/core/lib/iomgr/executor.c
  ${BASE}/src/core/lib/iomgr/iocp_windows.c
  ${BASE}/src/core/lib/iomgr/iomgr.c
  ${BASE}/src/core/lib/iomgr/iomgr_posix.c
  ${BASE}/src/core/lib/iomgr/iomgr_uv.c
  ${BASE}/src/core/lib/iomgr/iomgr_windows.c
  ${BASE}/src/core/lib/iomgr/load_file.c
  ${BASE}/src/core/lib/iomgr/network_status_tracker.c
  ${BASE}/src/core/lib/iomgr/polling_entity.c
  ${BASE}/src/core/lib/iomgr/pollset_set_uv.c
  ${BASE}/src/core/lib/iomgr/pollset_set_windows.c
  ${BASE}/src/core/lib/iomgr/pollset_uv.c
  ${BASE}/src/core/lib/iomgr/pollset_windows.c
  ${BASE}/src/core/lib/iomgr/resolve_address_posix.c
  ${BASE}/src/core/lib/iomgr/resolve_address_uv.c
  ${BASE}/src/core/lib/iomgr/resolve_address_windows.c
  ${BASE}/src/core/lib/iomgr/resource_quota.c
  ${BASE}/src/core/lib/iomgr/sockaddr_utils.c
  ${BASE}/src/core/lib/iomgr/socket_mutator.c
  ${BASE}/src/core/lib/iomgr/socket_utils_common_posix.c
  ${BASE}/src/core/lib/iomgr/socket_utils_linux.c
  ${BASE}/src/core/lib/iomgr/socket_utils_posix.c
  ${BASE}/src/core/lib/iomgr/socket_utils_uv.c
  ${BASE}/src/core/lib/iomgr/socket_utils_windows.c
  ${BASE}/src/core/lib/iomgr/socket_windows.c
  ${BASE}/src/core/lib/iomgr/tcp_client_posix.c
  ${BASE}/src/core/lib/iomgr/tcp_client_uv.c
  ${BASE}/src/core/lib/iomgr/tcp_client_windows.c
  ${BASE}/src/core/lib/iomgr/tcp_posix.c
  ${BASE}/src/core/lib/iomgr/tcp_server_posix.c
  ${BASE}/src/core/lib/iomgr/tcp_server_uv.c
  ${BASE}/src/core/lib/iomgr/tcp_server_windows.c
  ${BASE}/src/core/lib/iomgr/tcp_uv.c
  ${BASE}/src/core/lib/iomgr/tcp_windows.c
  ${BASE}/src/core/lib/iomgr/time_averaged_stats.c
  ${BASE}/src/core/lib/iomgr/timer_generic.c
  ${BASE}/src/core/lib/iomgr/timer_heap.c
  ${BASE}/src/core/lib/iomgr/timer_uv.c
  ${BASE}/src/core/lib/iomgr/udp_server.c
  ${BASE}/src/core/lib/iomgr/unix_sockets_posix.c
  ${BASE}/src/core/lib/iomgr/unix_sockets_posix_noop.c
  ${BASE}/src/core/lib/iomgr/wakeup_fd_cv.c
  ${BASE}/src/core/lib/iomgr/wakeup_fd_eventfd.c
  ${BASE}/src/core/lib/iomgr/wakeup_fd_nospecial.c
  ${BASE}/src/core/lib/iomgr/wakeup_fd_pipe.c
  ${BASE}/src/core/lib/iomgr/wakeup_fd_posix.c
  ${BASE}/src/core/lib/iomgr/workqueue_uv.c
  ${BASE}/src/core/lib/iomgr/workqueue_windows.c
  ${BASE}/src/core/lib/json/json.c
  ${BASE}/src/core/lib/json/json_reader.c
  ${BASE}/src/core/lib/json/json_string.c
  ${BASE}/src/core/lib/json/json_writer.c
  ${BASE}/src/core/lib/profiling/basic_timers.c
  ${BASE}/src/core/lib/profiling/stap_timers.c
  ${BASE}/src/core/lib/security/context/security_context.c
  ${BASE}/src/core/lib/security/credentials/composite/composite_credentials.c
  ${BASE}/src/core/lib/security/credentials/credentials.c
  ${BASE}/src/core/lib/security/credentials/credentials_metadata.c
  ${BASE}/src/core/lib/security/credentials/fake/fake_credentials.c
  ${BASE}/src/core/lib/security/credentials/google_default/credentials_generic.c
  ${BASE}/src/core/lib/security/credentials/google_default/google_default_credentials.c
  ${BASE}/src/core/lib/security/credentials/iam/iam_credentials.c
  ${BASE}/src/core/lib/security/credentials/jwt/json_token.c
  ${BASE}/src/core/lib/security/credentials/jwt/jwt_credentials.c
  ${BASE}/src/core/lib/security/credentials/jwt/jwt_verifier.c
  ${BASE}/src/core/lib/security/credentials/oauth2/oauth2_credentials.c
  ${BASE}/src/core/lib/security/credentials/plugin/plugin_credentials.c
  ${BASE}/src/core/lib/security/credentials/ssl/ssl_credentials.c
  ${BASE}/src/core/lib/security/transport/client_auth_filter.c
  ${BASE}/src/core/lib/security/transport/secure_endpoint.c
  ${BASE}/src/core/lib/security/transport/security_connector.c
  ${BASE}/src/core/lib/security/transport/security_handshaker.c
  ${BASE}/src/core/lib/security/transport/server_auth_filter.c
  ${BASE}/src/core/lib/security/transport/tsi_error.c
  ${BASE}/src/core/lib/security/util/b64.c
  ${BASE}/src/core/lib/security/util/json_util.c
  ${BASE}/src/core/lib/slice/percent_encoding.c
  ${BASE}/src/core/lib/slice/slice_buffer.c
  ${BASE}/src/core/lib/slice/slice.c
  ${BASE}/src/core/lib/slice/slice_string_helpers.c
  ${BASE}/src/core/lib/support/alloc.c
  ${BASE}/src/core/lib/support/avl.c
  ${BASE}/src/core/lib/support/backoff.c
  ${BASE}/src/core/lib/support/cmdline.c
  ${BASE}/src/core/lib/support/cpu_iphone.c
  ${BASE}/src/core/lib/support/cpu_linux.c
  ${BASE}/src/core/lib/support/cpu_posix.c
  ${BASE}/src/core/lib/support/cpu_windows.c
  ${BASE}/src/core/lib/support/env_linux.c
  ${BASE}/src/core/lib/support/env_posix.c
  ${BASE}/src/core/lib/support/env_windows.c
  ${BASE}/src/core/lib/support/histogram.c
  ${BASE}/src/core/lib/support/host_port.c
  ${BASE}/src/core/lib/support/log_android.c
  ${BASE}/src/core/lib/support/log.c
  ${BASE}/src/core/lib/support/log_linux.c
  ${BASE}/src/core/lib/support/log_posix.c
  ${BASE}/src/core/lib/support/log_windows.c
  ${BASE}/src/core/lib/support/mpscq.c
  ${BASE}/src/core/lib/support/murmur_hash.c
  ${BASE}/src/core/lib/support/stack_lockfree.c
  ${BASE}/src/core/lib/support/string.c
  ${BASE}/src/core/lib/support/string_posix.c
  ${BASE}/src/core/lib/support/string_util_windows.c
  ${BASE}/src/core/lib/support/string_windows.c
  ${BASE}/src/core/lib/support/subprocess_posix.c
  ${BASE}/src/core/lib/support/subprocess_windows.c
  ${BASE}/src/core/lib/support/sync.c
  ${BASE}/src/core/lib/support/sync_posix.c
  ${BASE}/src/core/lib/support/sync_windows.c
  ${BASE}/src/core/lib/support/thd.c
  ${BASE}/src/core/lib/support/thd_posix.c
  ${BASE}/src/core/lib/support/thd_windows.c
  ${BASE}/src/core/lib/support/time.c
  ${BASE}/src/core/lib/support/time_posix.c
  ${BASE}/src/core/lib/support/time_precise.c
  ${BASE}/src/core/lib/support/time_windows.c
  ${BASE}/src/core/lib/support/tls_pthread.c
  ${BASE}/src/core/lib/support/tmpfile_msys.c
  ${BASE}/src/core/lib/support/tmpfile_posix.c
  ${BASE}/src/core/lib/support/tmpfile_windows.c
  ${BASE}/src/core/lib/support/wrap_memcpy.c
  ${BASE}/src/core/lib/surface/alarm.c
  ${BASE}/src/core/lib/surface/api_trace.c
  ${BASE}/src/core/lib/surface/byte_buffer.c
  ${BASE}/src/core/lib/surface/byte_buffer_reader.c
  ${BASE}/src/core/lib/surface/call.c
  ${BASE}/src/core/lib/surface/call_details.c
  ${BASE}/src/core/lib/surface/call_log_batch.c
  ${BASE}/src/core/lib/surface/channel.c
  ${BASE}/src/core/lib/surface/channel_init.c
  ${BASE}/src/core/lib/surface/channel_ping.c
  ${BASE}/src/core/lib/surface/channel_stack_type.c
  ${BASE}/src/core/lib/surface/completion_queue.c
  ${BASE}/src/core/lib/surface/event_string.c
  ${BASE}/src/core/lib/surface/init.c
  #${BASE}/src/core/lib/surface/init_secure.c
  ${BASE}/src/core/lib/surface/init_unsecure.c
  ${BASE}/src/core/lib/surface/lame_client.c
  ${BASE}/src/core/lib/surface/metadata_array.c
  ${BASE}/src/core/lib/surface/server.c
  ${BASE}/src/core/lib/surface/validate_metadata.c
  ${BASE}/src/core/lib/surface/version.c
  ${BASE}/src/core/lib/transport/byte_stream.c
  ${BASE}/src/core/lib/transport/connectivity_state.c
  ${BASE}/src/core/lib/transport/mdstr_hash_table.c
  ${BASE}/src/core/lib/transport/metadata_batch.c
  ${BASE}/src/core/lib/transport/metadata.c
  #${BASE}/src/core/lib/transport/method_config.c
  ${BASE}/src/core/lib/transport/pid_controller.c
  ${BASE}/src/core/lib/transport/service_config.c
  ${BASE}/src/core/lib/transport/static_metadata.c
  ${BASE}/src/core/lib/transport/timeout_encoding.c
  ${BASE}/src/core/lib/transport/transport.c
  ${BASE}/src/core/lib/transport/transport_op_string.c
  ${BASE}/src/core/lib/tsi/fake_transport_security.c
  ${BASE}/src/core/lib/tsi/ssl_transport_security.c
  ${BASE}/src/core/lib/tsi/transport_security.c

  #${BASE}/src/core/plugin_registry/grpc_cronet_plugin_registry.c
  #${BASE}/src/core/plugin_registry/grpc_plugin_registry.c
  ${BASE}/src/core/plugin_registry/grpc_unsecure_plugin_registry.c
)

SET(DEPENDENCIES
  deps-arcadia-contrib-libs-nanopb
  deps-arcadia-contrib-libs-openssl
  deps-arcadia-contrib-libs-zlib
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${BASE}
  ${BASE}/include
  ${CMAKE_SOURCE_DIR}/contrib/libs/openssl/include
)


# Static library

add_library(${NAME}-core STATIC ${SRCS_CORE})

target_link_libraries(${NAME}-core
  ${DEPENDENCIES}
)

set_source_files_properties(${SRCS_CORE} PROPERTIES LANGUAGE C)
set_target_properties(${NAME}-core PROPERTIES COMPILE_FLAGS --std=c99)


# Python

if (${YT_BUILD_HAVE_CYTHON})
  check_python_kind("2_7" "/usr/include/python2.7")
  cython(${BASE}/src/python/grpcio/grpc/_cython/cygrpc)
  target_link_libraries(cygrpc
    ${NAME}-core
    -lrt
  )

  exec_program(
    ${CMAKE_COMMAND} ARGS -E create_symlink
    ${BASE}/src/python/grpcio/grpc
    ${CMAKE_SOURCE_DIR}/python/grpc
  )
  exec_program(
    ${CMAKE_COMMAND} ARGS -E create_symlink
    ${LIBRARY_OUTPUT_PATH}/libcygrpc.so
    ${CMAKE_SOURCE_DIR}/python/grpc/_cython/cygrpc.so
  )


  # Python protoc plugin

  add_executable(grpc_python
    ${BASE}/src/compiler/python_plugin.cc
    ${BASE}/src/compiler/python_generator.cc
  )

  SET(PROTOBUF_DIR ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf)
  set_target_properties(grpc_python
    PROPERTIES
    COMPILE_FLAGS "-I${PROTOBUF_DIR} -I${PROTOBUF_DIR}/google/protobuf"
  )
  target_link_libraries(grpc_python
    deps-arcadia-contrib-libs-protobuf-protoc
  )
endif()
