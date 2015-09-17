#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/unittests )
set( SRCS
  ${BASE}/any_ut.cpp
  ${BASE}/attributes_ut.cpp
  ${BASE}/bind_ut.cpp
  ${BASE}/bus_ut.cpp
  ${BASE}/callback_ut.cpp
  ${BASE}/cgroup_ut.cpp
  ${BASE}/checkpointable_stream_ut.cpp
  ${BASE}/chunk_tree_balancer_ut.cpp
  ${BASE}/chunk_tree_traversing_ut.cpp
  ${BASE}/compression_ut.cpp
  ${BASE}/coroutines_ut.cpp
  ${BASE}/dsv_parser_ut.cpp
  ${BASE}/dsv_writer_ut.cpp
  ${BASE}/dynamic_memory_store_ut.cpp
  ${BASE}/election_ut.cpp
  ${BASE}/enum_ut.cpp
  ${BASE}/erasure_ut.cpp
  ${BASE}/file_changelog_ut.cpp
  ${BASE}/fls_ut.cpp
  ${BASE}/format_ut.cpp
  ${BASE}/framework.cpp
  ${BASE}/future_ut.cpp
  ${BASE}/guid_ut.cpp
  ${BASE}/hash_filter_ut.cpp
  ${BASE}/heap_ut.cpp
  ${BASE}/intrusive_ptr_ut.cpp
  ${BASE}/json_parser_ut.cpp
  ${BASE}/json_writer_ut.cpp
  ${BASE}/lexer_ut.cpp
  ${BASE}/lifecycle_ut.cpp
  ${BASE}/logging_ut.cpp
  ${BASE}/main.cpp
  ${BASE}/memory_store_pt.cpp
  ${BASE}/mpl_ut.cpp
  ${BASE}/network_address_ut.cpp
  ${BASE}/nonblocking_queue_ut.cpp
  ${BASE}/nullable_ut.cpp
  ${BASE}/pattern_formatter_ut.cpp
  ${BASE}/phoenix_ut.cpp
  ${BASE}/pipes_ut.cpp
  ${BASE}/preprocessor_ut.cpp
  ${BASE}/process_ut.cpp
  ${BASE}/ql_helpers.cpp
  ${BASE}/ql_range_inference_ut.cpp
  ${BASE}/ql_expressions_ut.cpp
  ${BASE}/ql_computed_columns_ut.cpp
  ${BASE}/ql_query_ut.cpp
  ${BASE}/random_ut.cpp
  ${BASE}/read_limit_ut.cpp
  ${BASE}/ref_counted_tracker_ut.cpp
  ${BASE}/row_merger_ut.cpp
  ${BASE}/row_ut.cpp
  ${BASE}/rpc_ut.cpp
  ${BASE}/scheduler_ut.cpp
  ${BASE}/schema_ut.cpp
  ${BASE}/schemaful_dsv_parser_ut.cpp
  ${BASE}/schemaful_dsv_writer_ut.cpp
  ${BASE}/schemaless_blocks_ut.cpp
  ${BASE}/schemaless_chunks_ut.cpp
  ${BASE}/serialize_ut.cpp
  ${BASE}/skip_list_ut.cpp
  ${BASE}/small_string_ut.cpp
  ${BASE}/small_vector_ut.cpp
  ${BASE}/statistics_ut.cpp
  ${BASE}/store_manager_ut.cpp
  ${BASE}/stracer_ut.cpp
  ${BASE}/string_ut.cpp
  ${BASE}/subprocess_ut.cpp
  ${BASE}/sync_file_changelog_ut.cpp
  ${BASE}/table_consumer_ut.cpp
  ${BASE}/thread_affinity_ut.cpp
  ${BASE}/tools_ut.cpp
  ${BASE}/tree_builder_ut.cpp
  ${BASE}/url_ut.cpp
  ${BASE}/variant_ut.cpp
  ${BASE}/varint_ut.cpp
  ${BASE}/versioned_blocks_ut.cpp
  ${BASE}/versioned_chunks_ut.cpp
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
  ${BASE}/persistent_queue_ut.cpp
  ${BASE}/hyperloglog_ut.cpp
  ${BASE}/udf/invalid_ir.h
)

protoc( ${BASE}/rpc_ut.proto SRCS )

udf_bc( ${BASE}/udf/sum_udf2.c SRCS )
udf_bc( ${BASE}/udf/malloc_udf.c SRCS )
udf_bc(
  ${BASE}/udf/test_udfs.c
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
  ${BASE}/udf/test_udfs_o.c
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
