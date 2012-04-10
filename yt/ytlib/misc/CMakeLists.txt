#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/misc )
set( SRCS
  ${BASE}/async_stream_state.cpp
  ${BASE}/backtrace.cpp
  ${BASE}/blob_output.cpp
  ${BASE}/checksum.cpp
  ${BASE}/codec.cpp
  ${BASE}/configurable.cpp
  ${BASE}/delayed_invoker.cpp
  ${BASE}/demangle.cpp
  ${BASE}/error.cpp
  ${BASE}/errortrace.cpp
  ${BASE}/fs.cpp
  ${BASE}/guid.cpp
  ${BASE}/home.cpp
  ${BASE}/lease_manager.cpp
  ${BASE}/linux.cpp
  ${BASE}/metric.cpp
  ${BASE}/new.cpp
  ${BASE}/pattern_formatter.cpp
  ${BASE}/periodic_invoker.cpp
  ${BASE}/protobuf_helpers.cpp
  ${BASE}/ref.cpp
  ${BASE}/ref_counted.cpp
  ${BASE}/ref_counted_tracker.cpp
  ${BASE}/semaphore.cpp
  ${BASE}/serialize.cpp
  ${BASE}/small_vector.cpp
  ${BASE}/source_location.cpp
  ${BASE}/thread.cpp
)
set( HDRS
  ${BASE}/arcadia_helper.h
  ${BASE}/assert.h
  ${BASE}/async_stream_state.h
  ${BASE}/backtrace.h
  ${BASE}/blob_output.h
  ${BASE}/cache-inl.h
  ${BASE}/cache.h
  ${BASE}/checksum.h
  ${BASE}/codec.h
  ${BASE}/common.h
  ${BASE}/configurable-inl.h
  ${BASE}/configurable.h
  ${BASE}/cyclic_buffer.h
  ${BASE}/delayed_invoker.h
  ${BASE}/demangle.h
  ${BASE}/enum.h
  ${BASE}/error.h
  ${BASE}/errortrace.h
  ${BASE}/foreach.h
  ${BASE}/fs.h
  ${BASE}/guid.h
  ${BASE}/hash.h
  ${BASE}/home.h
  ${BASE}/id_generator.h
  ${BASE}/intrusive_ptr.h
  ${BASE}/lazy_holder.h
  ${BASE}/lazy_ptr.h
  ${BASE}/lease_manager.h
  ${BASE}/linux.h
  ${BASE}/metric.h
  ${BASE}/mpl.h
  ${BASE}/new.h
  ${BASE}/nullable.h
  ${BASE}/pattern_formatter.h
  ${BASE}/periodic_invoker.h
  ${BASE}/preprocessor-gen.h
  ${BASE}/preprocessor.h
  ${BASE}/property.h
  ${BASE}/protobuf_helpers-inl.h
  ${BASE}/protobuf_helpers.h
  ${BASE}/ref.h
  ${BASE}/ref_counted.h
  ${BASE}/ref_counted_tracker.h
  ${BASE}/rvalue.h
  ${BASE}/semaphore.h
  ${BASE}/serialize.h
  ${BASE}/singleton.h
  ${BASE}/small_set.h
  ${BASE}/small_string.h
  ${BASE}/small_vector.h
  ${BASE}/source_location.h
  ${BASE}/string.h
  ${BASE}/sync.h
  ${BASE}/tclap_helpers.h
  ${BASE}/thread.h
  ${BASE}/thread_affinity.h
  ${BASE}/weak_ptr.h
  ${BASE}/zigzag.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/guid.proto YT_SOURCE_FILES )

add_custom_command(
    OUTPUT  ${BASE}/preprocessor-gen.h
    COMMAND ${PYTHON_EXECUTABLE} ${BASE}/preprocessor-gen.py > ${BASE}/preprocessor-gen.h
    DEPENDS ${BASE}/preprocessor-gen.py
    COMMENT "Regenerating preprocessor-gen.h..."
)

PUMP( ${BASE}/new.h.pump YT_SOURCE_FILES )

protoc( ${BASE}/error.proto YT_SOURCE_FILES )
protoc( ${BASE}/extensions.proto YT_SOURCE_FILES )
