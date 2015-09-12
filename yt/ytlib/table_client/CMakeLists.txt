#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cached_versioned_chunk_meta.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_meta_extensions.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_reader_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_splits_fetcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_writer_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/legacy_channel_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/legacy_table_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/name_table.cpp
  ${CMAKE_CURRENT_LIST_DIR}/partition_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/partitioner.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipe.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/row_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/row_buffer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/row_sampler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/row_merger.cpp
  ${CMAKE_CURRENT_LIST_DIR}/samples_fetcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schema.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_block_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_block_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_reader_adapter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_writer_adapter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_block_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_block_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_buffered_table_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_partition_sort_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_row_reorderer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_sorted_merging_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_sorting_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/unordered_schemaful_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/unversioned_value.cpp
  ${CMAKE_CURRENT_LIST_DIR}/unversioned_row.cpp
  ${CMAKE_CURRENT_LIST_DIR}/versioned_block_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/versioned_block_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/versioned_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/versioned_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/versioned_row.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/block_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/cached_versioned_chunk_meta.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_meta_extensions.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_reader_base.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_splits_fetcher.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_writer_base.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/legacy_channel_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/legacy_table_chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/llvm_types.h
  ${CMAKE_CURRENT_LIST_DIR}/name_table.h
  ${CMAKE_CURRENT_LIST_DIR}/partition_chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/partition_chunk_reader-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/partitioner.h
  ${CMAKE_CURRENT_LIST_DIR}/pipe.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/reader.h
  ${CMAKE_CURRENT_LIST_DIR}/row_base.h
  ${CMAKE_CURRENT_LIST_DIR}/row_buffer.h
  ${CMAKE_CURRENT_LIST_DIR}/row_sampler.h
  ${CMAKE_CURRENT_LIST_DIR}/row_merger.h
  ${CMAKE_CURRENT_LIST_DIR}/samples_fetcher.h
  ${CMAKE_CURRENT_LIST_DIR}/schema.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_block_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_block_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_chunk_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_reader_adapter.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_writer_adapter.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_block_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_block_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_buffered_table_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_chunk_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_partition_sort_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_row_reorderer.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_sorted_merging_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_sorting_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/table_consumer.h
  ${CMAKE_CURRENT_LIST_DIR}/table_ypath_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/unordered_schemaful_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/unversioned_value.h
  ${CMAKE_CURRENT_LIST_DIR}/unversioned_row.h
  ${CMAKE_CURRENT_LIST_DIR}/unversioned_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/versioned_block_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/versioned_block_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/versioned_chunk_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/versioned_chunk_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/versioned_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/versioned_row.h
  ${CMAKE_CURRENT_LIST_DIR}/versioned_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/writer.h
)

if (YT_BUILD_ENABLE_LLVM)
  set( SRCS
    ${SRCS}
  )
  set( HDRS
    ${HDRS}
    ${CMAKE_CURRENT_LIST_DIR}/llvm_types.h
  )
endif()

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/chunk_meta.proto YT_SOURCE_FILES )

protoc( ${CMAKE_CURRENT_LIST_DIR}/legacy_chunk_meta.proto YT_SOURCE_FILES )

protoc( ${CMAKE_CURRENT_LIST_DIR}/table_ypath.proto YT_SOURCE_FILES )

