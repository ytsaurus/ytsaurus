IF(YMAKE)
  SET(CMAKE_CURRENT_LIST_DIR .)
  SET(YT_ABI 19_1)
ENDIF()

SET(PROTO_NAMESPACE yt/${YT_ABI})

SET(YT_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/actions/cancelable_context.cpp
  ${CMAKE_CURRENT_LIST_DIR}/actions/future.cpp
  ${CMAKE_CURRENT_LIST_DIR}/actions/invoker_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/actions/invoker_util.cpp

  ${CMAKE_CURRENT_LIST_DIR}/bus/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus/packet.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus/tcp_client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus/tcp_connection.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus/tcp_dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus/tcp_dispatcher_impl.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus/tcp_server.cpp

  ${CMAKE_CURRENT_LIST_DIR}/codegen/init.cpp
  ${CMAKE_CURRENT_LIST_DIR}/codegen/module.cpp
  ${CMAKE_CURRENT_LIST_DIR}/codegen/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/codegen/routine_registry.cpp

  ${CMAKE_CURRENT_LIST_DIR}/compression/brotli.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/brotli_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/bzip2.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/codec.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/details.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/lz.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/lzma.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/snappy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/zlib.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/zstd.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression/zstd_legacy.cpp

  ${CMAKE_CURRENT_LIST_DIR}/concurrency/action_queue.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/async_semaphore.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/async_rw_lock.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/async_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/async_stream_pipe.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/coroutine.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/delayed_executor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/ev_scheduler_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/execution_context.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/execution_stack.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/fair_share_action_queue.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/fair_share_invoker_queue.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/fair_share_queue_scheduler_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/fiber.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/finalizer_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/fls.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/fork_aware_spinlock.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/invoker_queue.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/periodic_executor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/periodic_yielder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/scheduler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/scheduler_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/single_queue_scheduler_thread.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/thread_pool.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/throughput_throttler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/lease_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/concurrency/count_down_latch.cpp

  ${CMAKE_CURRENT_LIST_DIR}/erasure/codec.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure/jerasure.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure/lrc.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure/reed_solomon.cpp

  ${CMAKE_CURRENT_LIST_DIR}/logging/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/logging/log.cpp
  ${CMAKE_CURRENT_LIST_DIR}/logging/log_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/logging/pattern.cpp
  ${CMAKE_CURRENT_LIST_DIR}/logging/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/logging/writer.cpp

  ${CMAKE_CURRENT_LIST_DIR}/misc/address.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/assert.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/async_stream_state.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/bitmap.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/blob.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/blob_output.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/bloom_filter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/bloom_filter.proto
  ${CMAKE_CURRENT_LIST_DIR}/misc/checkpointable_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/checksum.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/chunked_memory_allocator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/chunked_memory_pool.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/chunked_output_stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/core_dumper.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/cpuid.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/crash_handler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/demangle.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/digest.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/error.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/error.proto
  ${CMAKE_CURRENT_LIST_DIR}/misc/fs.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/guid.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/guid.proto
  ${CMAKE_CURRENT_LIST_DIR}/misc/high_resolution_clock.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/histogram.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/hr_timer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/id_generator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/lfalloc_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/lifecycle.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/linear_probe.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/new.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/pattern_formatter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/phoenix.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/proc.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/process.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/protobuf_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/protobuf_helpers.proto
  ${CMAKE_CURRENT_LIST_DIR}/misc/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/random.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/ref.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/ref_counted.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/ref_counted_tracker.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/safe_assert.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/small_vector.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/source_location.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/stack_trace.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/string.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/subprocess.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/url.cpp

  ${CMAKE_CURRENT_LIST_DIR}/pipes/async_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipes/async_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipes/io_dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipes/io_dispatcher_impl.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipes/pipe.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipes/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipes/pty.cpp

  ${CMAKE_CURRENT_LIST_DIR}/profiling/profile_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/profiling/profiler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/profiling/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/profiling/resource_tracker.cpp
  ${CMAKE_CURRENT_LIST_DIR}/profiling/timing.cpp

  ${CMAKE_CURRENT_LIST_DIR}/rpc/balancing_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/bus_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/bus_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/caching_channel_factory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/channel_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/local_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/local_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/message.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/redirector_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/response_keeper.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/retrying_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/roaming_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/rpc.proto
  ${CMAKE_CURRENT_LIST_DIR}/rpc/serialized_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/server_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/service_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/static_channel_factory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/throttling_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc/latency_taming_channel.cpp

  ${CMAKE_CURRENT_LIST_DIR}/tools/registry.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tools/tools.cpp

  ${CMAKE_CURRENT_LIST_DIR}/tracing/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tracing/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tracing/trace_context.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tracing/trace_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tracing/trace_service.proto

  ${CMAKE_CURRENT_LIST_DIR}/utilex/random.cpp

  ${CMAKE_CURRENT_LIST_DIR}/ypath/token.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ypath/tokenizer.cpp

  ${CMAKE_CURRENT_LIST_DIR}/yson/async_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/async_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/attribute_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/forwarding_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/lexer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/null_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/producer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/string.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/token.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/tokenizer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson/writer.cpp

  ${CMAKE_CURRENT_LIST_DIR}/ytree/attribute_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/attributes.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/attributes.proto
  ${CMAKE_CURRENT_LIST_DIR}/ytree/convert.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/ephemeral_attribute_owner.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/ephemeral_node_factory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/exception_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/node.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/node_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/permission.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/system_attribute_provider.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/tree_builder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/tree_visitor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/virtual.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/service_combiner.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/ytree/ypath_client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/ypath_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/ypath_resolver.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/ypath_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree/yson_serializable.cpp
)

IF(NOT OS_WINDOWS)
  SET(YT_SRCS ${YT_SRCS} ${CMAKE_CURRENT_LIST_DIR}/concurrency/execution_context-supp.S)
ENDIF()

################################################################################
IF(YMAKE)
################################################################################

LIBRARY()

OWNER(sandello)

NO_WERROR()

CXXFLAGS(
  -std=c++1y
  -Wno-sign-compare
  -Wno-unused-function
  -Wno-unused-parameter
  -Wno-unused-value
  -Wno-c++14-extensions
  -DYT_IN_ARCADIA
)

ADDINCL(
  contrib/libs/llvm37/include
)

SRCS(${YT_SRCS})

PEERDIR(
  contrib/libs/llvm37/lib
  contrib/libs/snappy
  contrib/libs/brotli/dec
  contrib/libs/brotli/enc
  contrib/libs/zstd07
  contrib/libs/lzmasdk
  yt/${YT_ABI}/yt/build
  yt/${YT_ABI}/yt/contrib
)

END()

################################################################################
ELSE()
################################################################################

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)
add_library(yt-core STATIC ${YT_SRCS_RESOLVED})

llvm_map_components_to_libraries(
  REQUIRED_LLVM_LIBRARIES
  mcjit ipo x86 mcdisassembler linker asmparser irreader instrumentation
)

# We link statically with zlib, and LLVM (sometimes) tries to bring its own
# dependency.
list(REMOVE_ITEM REQUIRED_LLVM_LIBRARIES "-lz")

target_link_libraries(yt-core
  deps-arcadia-contrib-libs-libbz2
  deps-arcadia-contrib-libs-lzmasdk
  deps-arcadia-contrib-libs-snappy
  deps-arcadia-contrib-libs-zstd07
  deps-arcadia-library-http
  deps-arcadia-library-streams-lz
  deps-arcadia-util
  deps-contrib-libs-brotli
  deps-yt-contrib-farmhash
  deps-yt-contrib-quicklz
  deps-yt-contrib-zstd-legacy
  protobuf
  yt-build
  yt-contrib-jerasure
  yt-contrib-libev
  ${REQUIRED_LLVM_LIBRARIES}
  deps-arcadia-contrib-libs-zlib # duplicate zlib dependency here, because LLVM depends on zlib
)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  target_link_libraries(yt-core deps-yt-contrib-coredumper)
endif()

if(CMAKE_COMPILER_IS_CLANG AND NOT APPLE)
  target_link_libraries(yt-core -latomic -ldl)
endif()

if(NOT APPLE)
  target_link_libraries(yt-core -lutil)
endif()

################################################################################
ENDIF()
################################################################################
