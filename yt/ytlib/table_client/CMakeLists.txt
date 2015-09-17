#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client )
set( SRCS
  ${BASE}/cached_versioned_chunk_meta.cpp
  ${BASE}/chunk_meta_extensions.cpp
  ${BASE}/chunk_reader_base.cpp
  ${BASE}/chunk_splits_fetcher.cpp
  ${BASE}/chunk_writer_base.cpp
  ${BASE}/helpers.cpp
  ${BASE}/legacy_channel_reader.cpp
  ${BASE}/legacy_table_chunk_reader.cpp
  ${BASE}/name_table.cpp
  ${BASE}/partition_chunk_reader.cpp
  ${BASE}/partitioner.cpp
  ${BASE}/pipe.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/row_base.cpp
  ${BASE}/row_buffer.cpp
  ${BASE}/row_sampler.cpp
  ${BASE}/row_merger.cpp
  ${BASE}/samples_fetcher.cpp
  ${BASE}/schema.cpp
  ${BASE}/schemaful_block_reader.cpp
  ${BASE}/schemaful_block_writer.cpp
  ${BASE}/schemaful_chunk_reader.cpp
  ${BASE}/schemaful_chunk_writer.cpp
  ${BASE}/schemaful_reader_adapter.cpp
  ${BASE}/schemaful_writer_adapter.cpp
  ${BASE}/schemaless_block_reader.cpp
  ${BASE}/schemaless_block_writer.cpp
  ${BASE}/schemaless_buffered_table_writer.cpp
  ${BASE}/schemaless_chunk_reader.cpp
  ${BASE}/schemaless_chunk_writer.cpp
  ${BASE}/schemaless_partition_sort_reader.cpp
  ${BASE}/schemaless_row_reorderer.cpp
  ${BASE}/schemaless_sorted_merging_reader.cpp
  ${BASE}/schemaless_sorting_reader.cpp
  ${BASE}/table_consumer.cpp
  ${BASE}/unordered_schemaful_reader.cpp
  ${BASE}/unversioned_value.cpp
  ${BASE}/unversioned_row.cpp
  ${BASE}/value_consumer.cpp
  ${BASE}/versioned_block_reader.cpp
  ${BASE}/versioned_block_writer.cpp
  ${BASE}/versioned_chunk_reader.cpp
  ${BASE}/versioned_chunk_writer.cpp
  ${BASE}/versioned_row.cpp
)

set( HDRS
  ${BASE}/block_writer.h
  ${BASE}/cached_versioned_chunk_meta.h
  ${BASE}/chunk_meta_extensions.h
  ${BASE}/chunk_reader_base.h
  ${BASE}/chunk_splits_fetcher.h
  ${BASE}/chunk_writer_base.h
  ${BASE}/config.h
  ${BASE}/helpers.h
  ${BASE}/legacy_channel_reader.h
  ${BASE}/legacy_table_chunk_reader.h
  ${BASE}/llvm_types.h
  ${BASE}/name_table.h
  ${BASE}/partition_chunk_reader.h
  ${BASE}/partition_chunk_reader-inl.h
  ${BASE}/partitioner.h
  ${BASE}/pipe.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/reader.h
  ${BASE}/row_base.h
  ${BASE}/row_buffer.h
  ${BASE}/row_sampler.h
  ${BASE}/row_merger.h
  ${BASE}/samples_fetcher.h
  ${BASE}/schema.h
  ${BASE}/schemaful_block_reader.h
  ${BASE}/schemaful_block_writer.h
  ${BASE}/schemaful_chunk_reader.h
  ${BASE}/schemaful_chunk_writer.h
  ${BASE}/schemaful_reader.h
  ${BASE}/schemaful_reader_adapter.h
  ${BASE}/schemaful_writer_adapter.h
  ${BASE}/schemaful_writer.h
  ${BASE}/schemaless_block_reader.h
  ${BASE}/schemaless_block_writer.h
  ${BASE}/schemaless_buffered_table_writer.h
  ${BASE}/schemaless_chunk_reader.h
  ${BASE}/schemaless_chunk_writer.h
  ${BASE}/schemaless_partition_sort_reader.h
  ${BASE}/schemaless_reader.h
  ${BASE}/schemaless_row_reorderer.h
  ${BASE}/schemaless_sorted_merging_reader.h
  ${BASE}/schemaless_sorting_reader.h
  ${BASE}/schemaless_writer.h
  ${BASE}/table_consumer.h
  ${BASE}/table_ypath_proxy.h
  ${BASE}/unordered_schemaful_reader.h
  ${BASE}/unversioned_value.h
  ${BASE}/unversioned_row.h
  ${BASE}/unversioned_writer.h
  ${BASE}/value_consumer.h
  ${BASE}/value_consumer-mock.h
  ${BASE}/versioned_block_reader.h
  ${BASE}/versioned_block_writer.h
  ${BASE}/versioned_chunk_reader.h
  ${BASE}/versioned_chunk_writer.h
  ${BASE}/versioned_reader.h
  ${BASE}/versioned_row.h
  ${BASE}/versioned_writer.h
  ${BASE}/writer.h
)

if (YT_BUILD_ENABLE_LLVM)
  set( SRCS
    ${SRCS}
  )
  set( HDRS
    ${HDRS}
    ${BASE}/llvm_types.h
  )
endif()

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_meta.proto YT_SOURCE_FILES )

protoc( ${BASE}/legacy_chunk_meta.proto YT_SOURCE_FILES )

protoc( ${BASE}/table_ypath.proto YT_SOURCE_FILES )

