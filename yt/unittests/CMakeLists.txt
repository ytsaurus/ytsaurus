#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/any_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/async_rw_lock_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/attributes_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bind_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bus_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/callback_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cgroup_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/checkpointable_stream_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_balancer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_tree_traversing_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/compression_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/coroutines_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dsv_parser_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dsv_writer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dynamic_memory_store_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/election_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/enum_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/erasure_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_changelog_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/fls_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/format_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/framework.cpp
  ${CMAKE_CURRENT_LIST_DIR}/future_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/guid_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hash_filter_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/heap_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/intrusive_ptr_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/json_parser_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/json_writer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lexer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lifecycle_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/logging_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/main.cpp
  ${CMAKE_CURRENT_LIST_DIR}/memory_store_pt.cpp
  ${CMAKE_CURRENT_LIST_DIR}/mpl_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/network_address_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/nonblocking_queue_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/nullable_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pattern_formatter_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/phoenix_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/pipes_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/preprocessor_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/process_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_range_inference_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_expressions_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_computed_columns_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_query_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/random_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/read_limit_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ref_counted_tracker_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/row_merger_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/row_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/rpc_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schema_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_dsv_parser_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaful_dsv_writer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_blocks_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/schemaless_chunks_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/skip_list_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/small_string_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/small_vector_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/statistics_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/store_manager_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/stracer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/string_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/subprocess_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/sync_file_changelog_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_consumer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/thread_affinity_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tools_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tree_builder_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/url_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/variant_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/varint_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/versioned_blocks_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/versioned_chunks_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/weak_ptr_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yamr_parser_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yamr_writer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yamred_dsv_parser_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yamred_dsv_writer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ypath_tokenizer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ypath_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson_parser_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson_serializable_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yson_writer_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ytree_fluent_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/yverify_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/zigzag_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/persistent_queue_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hyperloglog_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/udf/invalid_ir.h
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/rpc_ut.proto SRCS )

udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/sum_udf2.c SRCS )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/malloc_udf.c SRCS )
udf_bc(
  ${CMAKE_CURRENT_LIST_DIR}/udf/test_udfs.c
  SRCS
  SYMBOLS
    seventyfive
    strtol_udf
    exp_udf
    tolower_udf
    is_null_udf
    abs_udf
    sum_udf
    throw_if_negative_udf
    avg_udaf_init
    avg_udaf_update
    avg_udaf_merge
    avg_udaf_finalize
)
udf_o(
  ${CMAKE_CURRENT_LIST_DIR}/udf/test_udfs_o.c
  SRCS
  SYMBOLS
    abs_udf
    abs_udf_o
    exp_udf
    max_udaf_init
    max_udaf_update
    max_udaf_merge
    max_udaf_finalize
)

add_msvc_precompiled_header(
  "stdafx.h"
  "stdafx.cpp"
  SRCS
)

if (MSVC)
  add_definitions( -D_VARIADIC_MAX=10 )
  add_definitions( -DGTEST_HAS_SEH=0 )
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
  ytext-lfalloc
)
