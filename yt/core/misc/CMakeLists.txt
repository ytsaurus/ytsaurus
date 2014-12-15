#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/misc )
set( SRCS
  ${BASE}/address.cpp
  ${BASE}/assert.cpp
  ${BASE}/async_stream_state.cpp
  ${BASE}/backtrace.cpp
  ${BASE}/blob.cpp
  ${BASE}/blob_output.cpp
  ${BASE}/blob_range.cpp
  ${BASE}/checksum.cpp
  ${BASE}/chunked_memory_pool.cpp
  ${BASE}/chunked_output_stream.cpp
  ${BASE}/crash_handler.cpp
  ${BASE}/demangle.cpp
  ${BASE}/error.cpp
  ${BASE}/fs.cpp
  ${BASE}/guid.cpp
  ${BASE}/high_resolution_clock.cpp
  ${BASE}/home.cpp
  ${BASE}/hr_timer.cpp
  ${BASE}/id_generator.cpp
  ${BASE}/lease_manager.cpp
  ${BASE}/lfalloc_helpers.cpp
  ${BASE}/metric.cpp
  ${BASE}/new.cpp
  ${BASE}/pattern_formatter.cpp
  ${BASE}/phoenix.cpp
  ${BASE}/proc.cpp
  ${BASE}/protobuf_helpers.cpp
  ${BASE}/random.cpp
  ${BASE}/ref.cpp
  ${BASE}/ref_counted.cpp
  ${BASE}/ref_counted_tracker.cpp
  ${BASE}/serialize.cpp
  ${BASE}/small_vector.cpp
  ${BASE}/source_location.cpp
  ${BASE}/stack_trace.cpp
  ${BASE}/string.cpp
  ${BASE}/tclap_helpers.cpp
  ${BASE}/url.cpp
)
set( HDRS
  ${BASE}/address.h
  ${BASE}/arcadia_helper.h
  ${BASE}/assert.h
  ${BASE}/async_stream_state.h
  ${BASE}/backtrace.h
  ${BASE}/blob.h
  ${BASE}/blob_output.h
  ${BASE}/blob_range.h
  ${BASE}/cache-inl.h
  ${BASE}/cache.h
  ${BASE}/checksum.h
  ${BASE}/chunked_memory_pool.h
  ${BASE}/chunked_output_stream.h
  ${BASE}/common.h
  ${BASE}/collection_helpers.h
  ${BASE}/collection_helpers-inl.h
  ${BASE}/crash_handler.h
  ${BASE}/demangle.h
  ${BASE}/enum.h
  ${BASE}/error.h
  ${BASE}/foreach.h
  ${BASE}/fs.h
  ${BASE}/guid.h
  ${BASE}/hash.h
  ${BASE}/heap-inl.h
  ${BASE}/heap.h
  ${BASE}/high_resolution_clock.h
  ${BASE}/home.h
  ${BASE}/hr_timer.h
  ${BASE}/id_generator.h
  ${BASE}/intrusive_ptr.h
  ${BASE}/lazy_holder.h
  ${BASE}/lazy_ptr.h
  ${BASE}/lease_manager.h
  ${BASE}/lfalloc_helpers.h
  ${BASE}/metric.h
  ${BASE}/mpl.h
  ${BASE}/nullable.h
  ${BASE}/object_pool-inl.h
  ${BASE}/object_pool.h
  ${BASE}/pattern_formatter.h
  ${BASE}/phoenix-inl.h
  ${BASE}/phoenix.h
  ${BASE}/preprocessor.h
  ${BASE}/proc.h
  ${BASE}/property.h
  ${BASE}/protobuf_helpers-inl.h
  ${BASE}/protobuf_helpers.h
  ${BASE}/random-inl.h
  ${BASE}/random.h
  ${BASE}/raw_formatter.h
  ${BASE}/ref.h
  ${BASE}/ref_counted.h
  ${BASE}/ref_counted_tracker.h
  ${BASE}/ref_tracked.h
  ${BASE}/ring_queue.h
  ${BASE}/serialize.h
  ${BASE}/singleton.h
  ${BASE}/small_set.h
  ${BASE}/small_string.h
  ${BASE}/small_vector.h
  ${BASE}/source_location.h
  ${BASE}/stack_trace.h
  ${BASE}/string.h
  ${BASE}/sync.h
  ${BASE}/tclap_helpers.h
  ${BASE}/url.h
  ${BASE}/weak_ptr.h
  ${BASE}/ioprio.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

if (YT_BUILD_ENABLE_BENCHMARKS)
  list(APPEND YT_SOURCE_FILES
    ${BASE}/benchmark.h
    ${BASE}/benchmark.cpp
  )
endif()

protoc( ${BASE}/guid.proto YT_SOURCE_FILES )
protoc( ${BASE}/protobuf_helpers.proto YT_SOURCE_FILES )
protoc( ${BASE}/error.proto YT_SOURCE_FILES )

PUMP( ${BASE}/new.h.pump YT_SOURCE_FILES )
PUMP( ${BASE}/preprocessor-gen.h.pump YT_SOURCE_FILES )
