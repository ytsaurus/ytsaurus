IF(YMAKE)
  SET(CMAKE_CURRENT_LIST_DIR .)
ENDIF()

SET(YT_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/api/admin.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/client.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/connection.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/file_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/file_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/journal_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/journal_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/rowset.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/table_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/api/transaction.cpp

  ${CMAKE_CURRENT_LIST_DIR}/cgroup/cgroup.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cgroup/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cgroup/private.cpp

  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/block_id.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_info.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_list_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_meta.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_meta_extensions.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_owner_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_replica.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_scraper.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_slice.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_slice.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_spec.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_spec.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_teleporter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/chunk_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/client_block_cache.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/confirming_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/data_node_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/data_statistics.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/data_statistics.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/encoding_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/encoding_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/erasure_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/erasure_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/fetcher_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/file_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/file_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/format.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/job.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/memory_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/memory_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/multi_chunk_reader_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/multi_chunk_writer_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/multi_reader_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/read_limit.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/reader_factory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/replication_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/replication_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/schema.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/schema.proto
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/sequential_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_client/throttler_manager.cpp

  ${CMAKE_CURRENT_LIST_DIR}/cypress_client/cypress_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/cypress_client/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/cypress_client/rpc.proto
  ${CMAKE_CURRENT_LIST_DIR}/cypress_client/rpc_helpers.cpp

  ${CMAKE_CURRENT_LIST_DIR}/driver/cypress_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/dispatcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/driver.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/etc_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/file_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/journal_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/scheduler_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/table_commands.cpp
  ${CMAKE_CURRENT_LIST_DIR}/driver/transaction_commands.cpp

  ${CMAKE_CURRENT_LIST_DIR}/election/cell_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/election/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/election/election_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/election/election_service_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/election/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/election/public.cpp

  ${CMAKE_CURRENT_LIST_DIR}/file_client/file_chunk_meta.proto
  ${CMAKE_CURRENT_LIST_DIR}/file_client/file_chunk_output.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_client/file_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_client/file_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/file_client/private.cpp

  ${CMAKE_CURRENT_LIST_DIR}/formats/dsv_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/dsv_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/format.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/json_callbacks.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/schemaful_dsv_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/schemaful_dsv_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/schemaful_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/schemaless_writer_adapter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/symbols.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/utf8_decoder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/yamr_base_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/yamr_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/yamr_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/yamr_writer_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/yamred_dsv_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/yamred_dsv_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/yson_parser.cpp

  ${CMAKE_CURRENT_LIST_DIR}/hive/cell_directory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hive/cell_directory.proto
  ${CMAKE_CURRENT_LIST_DIR}/hive/cluster_directory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hive/hive_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/hive/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hive/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hive/transaction_supervisor_service.proto

  ${CMAKE_CURRENT_LIST_DIR}/hydra/hydra_manager.proto
  ${CMAKE_CURRENT_LIST_DIR}/hydra/hydra_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/hydra/peer_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hydra/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hydra/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hydra/version.cpp

  ${CMAKE_CURRENT_LIST_DIR}/job_prober_client/job_prober_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/job_prober_client/job_signal.cpp

  ${CMAKE_CURRENT_LIST_DIR}/job_tracker_client/job.proto
  ${CMAKE_CURRENT_LIST_DIR}/job_tracker_client/job_tracker_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/job_tracker_client/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/job_tracker_client/statistics.cpp

  ${CMAKE_CURRENT_LIST_DIR}/journal_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/journal_client/journal_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/journal_client/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/journal_client/public.cpp

  ${CMAKE_CURRENT_LIST_DIR}/misc/workload.cpp
  ${CMAKE_CURRENT_LIST_DIR}/misc/workload.proto

  ${CMAKE_CURRENT_LIST_DIR}/monitoring/http_integration.cpp
  ${CMAKE_CURRENT_LIST_DIR}/monitoring/http_server.cpp
  ${CMAKE_CURRENT_LIST_DIR}/monitoring/monitoring_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/monitoring/private.cpp

  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_client/node.proto
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_client/node_directory.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_client/node_directory_builder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_client/node_statistics.cpp
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_client/node_tracker_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/node_tracker_client/public.cpp

  ${CMAKE_CURRENT_LIST_DIR}/object_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object_client/master_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/object_client/object_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/object_client/object_service_proxy.cpp
  ${CMAKE_CURRENT_LIST_DIR}/object_client/object_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/object_client/public.cpp

  ${CMAKE_CURRENT_LIST_DIR}/orchid/orchid_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/orchid/orchid_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/orchid/private.cpp

  ${CMAKE_CURRENT_LIST_DIR}/query_client/ast.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/builtin_functions.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/cg_fragment_compiler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/cg_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/cg_ir_builder.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/cg_routines.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/column_evaluator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/coordinator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/evaluation_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/evaluator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/folding_profiler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/function_context.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/function_registry.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/functions.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/key_trie.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/lexer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/plan_fragment.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/plan_fragment.proto
  ${CMAKE_CURRENT_LIST_DIR}/query_client/plan_fragment_common.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/plan_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/query_preparer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/query_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/query_client/query_statistics.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/query_statistics.proto
  ${CMAKE_CURRENT_LIST_DIR}/query_client/range_inferrer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/udf_descriptor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_client/user_defined_functions.cpp

  ${CMAKE_CURRENT_LIST_DIR}/scheduler/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler/job.proto
  ${CMAKE_CURRENT_LIST_DIR}/scheduler/job_prober_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/scheduler/scheduler_channel.cpp
  ${CMAKE_CURRENT_LIST_DIR}/scheduler/scheduler_service.proto

  ${CMAKE_CURRENT_LIST_DIR}/security_client/account_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/security_client/group_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/security_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/security_client/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/security_client/user_ypath.proto

  ${CMAKE_CURRENT_LIST_DIR}/table_client/cached_versioned_chunk_meta.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/chunk_meta.proto
  ${CMAKE_CURRENT_LIST_DIR}/table_client/chunk_meta_extensions.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/chunk_reader_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/chunk_slices_fetcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/chunk_writer_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/legacy_channel_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/legacy_chunk_meta.proto
  ${CMAKE_CURRENT_LIST_DIR}/table_client/legacy_table_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/name_table.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/partition_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/partitioner.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/pipe.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/row_base.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/row_buffer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/row_merger.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/row_sampler.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/samples_fetcher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schema.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaful_block_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaful_block_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaful_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaful_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaful_overlapping_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaful_reader_adapter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaful_writer_adapter.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_block_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_block_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_buffered_table_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_partition_sort_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_row_reorderer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_sorted_merging_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/schemaless_sorting_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/table_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/table_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/table_client/unordered_schemaful_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/unversioned_row.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/unversioned_value.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/value_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/versioned_block_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/versioned_block_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/versioned_chunk_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/versioned_chunk_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/table_client/versioned_row.cpp

  ${CMAKE_CURRENT_LIST_DIR}/tablet_client/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_client/public.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_client/table_mount_cache.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_client/tablet_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/tablet_client/wire_protocol.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_client/wire_protocol.proto

  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/remote_timestamp_provider.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/timestamp_service.proto
  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/transaction_listener.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/transaction_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/transaction_ypath.proto
  ${CMAKE_CURRENT_LIST_DIR}/transaction_client/transaction_ypath_proxy.cpp

  ${CMAKE_CURRENT_LIST_DIR}/ypath/rich.cpp

  ${CMAKE_CURRENT_LIST_DIR}/shutdown.cpp
)

################################################################################
IF(YMAKE)
################################################################################

LIBRARY()

ADDINCL(
  contrib/libs/llvm37/include
  contrib/libs/sparsehash/src
  contrib/libs/re2
)

SRCS(
  ${YT_SRCS}
  ${CMAKE_CURRENT_LIST_DIR}/formats/json_parser_arc.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/json_writer_arc.cpp
)

CXXFLAGS(
  -std=c++1y
  -DYMAKE
  -Wno-sign-compare
  -Wno-unused-parameter
  -Wno-unused-value
  -Wno-c++14-extensions
)

PEERDIR(
  contrib/libs/llvm37/lib
  contrib/libs/re2
  yt/core
)

END()

################################################################################
ELSE()
################################################################################

set(YT_SOURCE_FILES)
include(query_client/CMakeLists.txt)

set(YT_SRCS
  ${YT_SRCS}
  ${CMAKE_CURRENT_LIST_DIR}/formats/json_parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/formats/json_writer.cpp
)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)
list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

add_library(ytlib STATIC ${YT_SRCS_RESOLVED})

target_link_libraries(ytlib
  ytcore
  deps-arcadia-contrib-libs-re2
)

################################################################################
ENDIF()
################################################################################

