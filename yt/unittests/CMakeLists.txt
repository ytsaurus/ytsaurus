#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/unittests )
set( SRCS
  #${BASE}/async_change_log_ut.cpp
  ${BASE}/future_ut.cpp
  ${BASE}/attributes_ut.cpp
  ${BASE}/bind_ut.cpp
  ${BASE}/bus_ut.cpp
  ${BASE}/callback_ut.cpp
  ${BASE}/checkpointable_stream_ut.cpp
  ${BASE}/sync_file_changelog_ut.cpp
  ${BASE}/cgroup_ut.cpp
  ${BASE}/chunk_tree_traversing_ut.cpp
  ${BASE}/compression_ut.cpp
  ${BASE}/coroutines_ut.cpp
  ${BASE}/dsv_parser_ut.cpp
  ${BASE}/dsv_writer_ut.cpp
  ${BASE}/enum_ut.cpp
  ${BASE}/erasure_ut.cpp
  ${BASE}/fibers_ut.cpp
  ${BASE}/foreach_ut.cpp
  ${BASE}/guid_ut.cpp
  ${BASE}/heap_ut.cpp
  ${BASE}/intrusive_ptr_ut.cpp
  ${BASE}/json_parser_ut.cpp
  ${BASE}/json_writer_ut.cpp
  ${BASE}/lexer_ut.cpp
  ${BASE}/logging_ut.cpp
  #${BASE}/meta_state_map_ut.cpp
  ${BASE}/mpl_ut.cpp
  ${BASE}/network_address_ut.cpp
  ${BASE}/nonblocking_queue_ut.cpp
  ${BASE}/nullable_ut.cpp
  ${BASE}/pattern_formatter_ut.cpp
  ${BASE}/phoenix_ut.cpp
  ${BASE}/pipes_ut.cpp
  ${BASE}/preprocessor_ut.cpp
  ${BASE}/process_ut.cpp
  ${BASE}/random_ut.cpp
  ${BASE}/ref_counted_tracker_ut.cpp
  ${BASE}/rpc_ut.cpp
  ${BASE}/schema_ut.cpp
  ${BASE}/schemaful_dsv_parser_ut.cpp
  ${BASE}/schemaful_dsv_writer_ut.cpp
  ${BASE}/serialize_ut.cpp
  ${BASE}/small_string_ut.cpp
  ${BASE}/small_vector_ut.cpp
  #${BASE}/snapshot_ut.cpp
  ${BASE}/string_ut.cpp
  ${BASE}/table_consumer_ut.cpp
  ${BASE}/thread_affinity_ut.cpp
  ${BASE}/tree_builder_ut.cpp
  ${BASE}/url_ut.cpp
  ${BASE}/main.cpp
  ${BASE}/varint_ut.cpp
  ${BASE}/versioned_blocks_ut.cpp
  ${BASE}/versioned_chunks_ut.cpp
  #${BASE}/versioned_table_client_ut.cpp
  ${BASE}/weak_ptr_ut.cpp
  ${BASE}/yamr_parser_ut.cpp
  ${BASE}/yamr_writer_ut.cpp
  ${BASE}/yamred_dsv_parser_ut.cpp
  ${BASE}/yamred_dsv_writer_ut.cpp
  ${BASE}/ypath_tokenizer_ut.cpp
  ${BASE}/ypath_ut.cpp
  ${BASE}/yson_parser_ut.cpp
  ${BASE}/yson_serializable_ut.cpp
  ${BASE}/yson_ut.cpp
  ${BASE}/yson_writer_ut.cpp
  ${BASE}/ytree_fluent_ut.cpp
  ${BASE}/yverify_ut.cpp
  ${BASE}/zigzag_ut.cpp
  ${BASE}/skip_list_ut.cpp
  ${BASE}/query_client_ut.cpp
  ${BASE}/row_ut.cpp
  ${BASE}/row_merger_ut.cpp
  ${BASE}/memory_store_ut.h
#  ${BASE}/static_memory_store_ut.cpp
  ${BASE}/dynamic_memory_store_ut.cpp
  ${BASE}/store_manager_ut.cpp
  ${BASE}/memory_store_pt.cpp
  ${BASE}/fls_ut.cpp
  ${BASE}/election_ut.cpp
  ${BASE}/scheduler_ut.cpp
  ${BASE}/variant_ut.cpp
  ${BASE}/any_ut.cpp
  ${BASE}/format_ut.cpp
  ${BASE}/framework.h
  ${BASE}/framework.cpp
)

protoc( ${BASE}/rpc_ut.proto SRCS )

add_msvc_precompiled_header(
  "stdafx.h"
  "stdafx.cpp"
  SRCS
)

if (MSVC)
  add_definitions( -D_VARIADIC_MAX=10 )
  add_definitions( -DGTEST_HAS_SEH=0 )
endif()

if (APPLE)
  add_definitions( -DGTEST_HAS_TR1_TUPLE=1 -DGTEST_USE_OWN_TR1_TUPLE=1 )
endif()

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable( unittester
  ${CMAKE_SOURCE_DIR}/contrib/testing/framework.cpp
  ${SRCS}
)

target_link_libraries( unittester
  ytlib
  ytserver_lib
)

if (WIN32)
  target_link_libraries( unittester
    ytext-lfalloc-fake
  )
else()
  target_link_libraries( unittester
    ytext-lfalloc
  )
endif()
