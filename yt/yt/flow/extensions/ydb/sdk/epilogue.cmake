
set(YT_FLOW_YDB_SDK_ADAPTER {{ target.name }})
get_target_property(YT_FLOW_YDB_SDK_DEPENDENCIES ${YT_FLOW_YDB_SDK_ADAPTER} INTERFACE_LINK_LIBRARIES)

if (NOT "cpp-monlib-dynamic_counters" IN_LIST YT_FLOW_YDB_SDK_DEPENDENCIES)
  add_library(yt-flow-ydb-sdk-counters STATIC)

  target_compile_options(yt-flow-ydb-sdk-counters PRIVATE
    -DNO_CUSTOM_CHAR_PTR_STD_COMPARATOR
  )

  target_link_libraries(yt-flow-ydb-sdk-counters PUBLIC
    ${YT_FLOW_YDB_SDK_DEPENDENCIES}
  )

  target_sources(yt-flow-ydb-sdk-counters PRIVATE
    ${PROJECT_SOURCE_DIR}/library/cpp/monlib/dynamic_counters/counters.cpp
    ${PROJECT_SOURCE_DIR}/library/cpp/monlib/dynamic_counters/encode.cpp
    ${PROJECT_SOURCE_DIR}/library/cpp/monlib/service/pages/templates.cpp
  )

  list(APPEND YT_FLOW_YDB_SDK_DEPENDENCIES
    yt-flow-ydb-sdk-counters
  )
endif()

set(YT_FLOW_YDB_SDK_PROTO_OUTS
  --cpp_out=${PROJECT_BINARY_DIR}/
)
if (vanilla_protobuf STREQUAL "yes")
  set(YT_FLOW_YDB_SDK_PROTOBUF_PROTO_ROOT ${protobuf_INCLUDE_DIRS})
else()
  set(YT_FLOW_YDB_SDK_PROTOBUF_PROTO_ROOT ${PROJECT_SOURCE_DIR}/contrib/libs/protobuf/src)
  list(APPEND YT_FLOW_YDB_SDK_PROTO_OUTS
    --cpp_styleguide_out=:${PROJECT_BINARY_DIR}/
  )
endif()

if (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC" OR CMAKE_CXX_COMPILER_FRONTEND_VARIANT STREQUAL "MSVC")
  set(YT_FLOW_YDB_SDK_MSVC_FRONTEND TRUE)
else()
  set(YT_FLOW_YDB_SDK_MSVC_FRONTEND FALSE)
endif()

if (vanilla_protobuf STREQUAL "yes")
  include(FetchContent)
  FetchContent_Declare(yt_flow_ydb_grpc_upstream
    URL https://codeload.github.com/grpc/grpc/tar.gz/refs/tags/v1.61.3
    URL_HASH SHA256=b62f9e7372e9700958b8af069650d915912a87772694849f99a3e3e691275688
  )
  FetchContent_GetProperties(yt_flow_ydb_grpc_upstream)
  if (NOT yt_flow_ydb_grpc_upstream_POPULATED)
    FetchContent_Populate(yt_flow_ydb_grpc_upstream)
  endif()

  set(YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_SRC_PATH yt/yt/flow/extensions/ydb/sdk/grpc_cpp_plugin)
  set(YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN yt-flow-ydb-grpc-cpp-plugin)

  set(YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_PREINCLUDE
    ${CMAKE_CURRENT_BINARY_DIR}/grpc_cpp_plugin/grpc_custom_protobuf.h
  )
  file(GENERATE
    OUTPUT ${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_PREINCLUDE}
    CONTENT "#pragma once\n\n#include <google/protobuf/util/json_util.h>\n\n#define GRPC_CUSTOM_JSONUTIL ::google::protobuf::util\n#define GRPC_CUSTOM_UTIL_STATUS ::google::protobuf::util::Status\n#define GRPC_CUSTOM_CSHARP_GETCLASSNAME ::google::protobuf::compiler::csharp::GetClassName\n"
  )
  if (YT_FLOW_YDB_SDK_MSVC_FRONTEND)
    set(YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_PREINCLUDE_OPTION
      "/FI${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_PREINCLUDE}"
    )
  else()
    set(YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_PREINCLUDE_OPTION
      "SHELL:-include \"${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_PREINCLUDE}\""
    )
  endif()

  add_executable(${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN})

  set_target_properties(${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN} PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_SRC_PATH}
  )

  target_compile_options(${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN} PRIVATE
    ${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_PREINCLUDE_OPTION}
  )

  target_include_directories(${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN} BEFORE PRIVATE
    ${yt_flow_ydb_grpc_upstream_SOURCE_DIR}
    ${yt_flow_ydb_grpc_upstream_SOURCE_DIR}/include
  )

  target_link_libraries(${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN} PRIVATE
    protobuf::libprotoc
    protobuf::libprotobuf
    contrib-libs-cxxsupp
  )

  target_sources(${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN} PRIVATE
    ${yt_flow_ydb_grpc_upstream_SOURCE_DIR}/src/compiler/cpp_plugin.cc
    ${yt_flow_ydb_grpc_upstream_SOURCE_DIR}/src/compiler/cpp_generator.cc
    ${yt_flow_ydb_grpc_upstream_SOURCE_DIR}/src/compiler/proto_parser_helper.cc
  )

  get_built_tool_path(
    YT_FLOW_YDB_SDK_GRPC_CPP_BIN
    YT_FLOW_YDB_SDK_GRPC_CPP_DEPENDENCY
    ${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN_SRC_PATH}
    ${YT_FLOW_YDB_SDK_GRPC_CPP_PLUGIN}
  )
else()
  get_built_tool_path(
    YT_FLOW_YDB_SDK_GRPC_CPP_BIN
    YT_FLOW_YDB_SDK_GRPC_CPP_DEPENDENCY
    contrib/tools/protoc/plugins/grpc_cpp
    grpc_cpp
  )
endif()

add_library(yt-flow-ydb-sdk-api-protos STATIC)

target_compile_options(yt-flow-ydb-sdk-api-protos PRIVATE
  -DNO_CUSTOM_CHAR_PTR_STD_COMPARATOR
)

target_link_libraries(yt-flow-ydb-sdk-api-protos PUBLIC
  ${YT_FLOW_YDB_SDK_DEPENDENCIES}
)

target_proto_messages(yt-flow-ydb-sdk-api-protos PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/annotations/sensitive.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/annotations/validation.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/draft/ydb_topic_deferred_publish.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_auth.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_bridge_common.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_common.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_coordination.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_discovery.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_export.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_federation_discovery.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_formats.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_import.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_issue_message.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_operation.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_persqueue_cluster_discovery.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_persqueue_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_query.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_query_stats.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_scheme.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_status_codes.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_table.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_topic.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/protos/ydb_value.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/sdk/cpp/src/library/operation_id/protos/operation_id.proto
)

target_proto_outs(yt-flow-ydb-sdk-api-protos
  ${YT_FLOW_YDB_SDK_PROTO_OUTS}
)

target_proto_addincls(yt-flow-ydb-sdk-api-protos
  ${PROJECT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${YT_FLOW_YDB_SDK_PROTOBUF_PROTO_ROOT}
)

generate_enum_serilization(yt-flow-ydb-sdk-api-protos
  ${PROJECT_BINARY_DIR}/contrib/ydb/public/api/protos/ydb_persqueue_cluster_discovery.pb.h
  INCLUDE_HEADERS
  contrib/ydb/public/api/protos/ydb_persqueue_cluster_discovery.pb.h
)

generate_enum_serilization(yt-flow-ydb-sdk-api-protos
  ${PROJECT_BINARY_DIR}/contrib/ydb/public/api/protos/ydb_topic.pb.h
  INCLUDE_HEADERS
  contrib/ydb/public/api/protos/ydb_topic.pb.h
)

add_library(yt-flow-ydb-sdk-api-grpc STATIC)

set_property(TARGET yt-flow-ydb-sdk-api-grpc PROPERTY PROTOC_EXTRA_OUTS
  .grpc.pb.cc
  .grpc.pb.h
)

target_compile_options(yt-flow-ydb-sdk-api-grpc PRIVATE
  -DNO_CUSTOM_CHAR_PTR_STD_COMPARATOR
)

target_link_libraries(yt-flow-ydb-sdk-api-grpc PUBLIC
  yt-flow-ydb-sdk-api-protos
)

target_proto_messages(yt-flow-ydb-sdk-api-grpc PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_auth_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_discovery_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_federation_discovery_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_operation_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_query_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_scheme_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_table_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/ydb_topic_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/draft/ydb_persqueue_v1.proto
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/api/grpc/draft/ydb_topic_deferred_publish_v1.proto
)

target_proto_outs(yt-flow-ydb-sdk-api-grpc
  ${YT_FLOW_YDB_SDK_PROTO_OUTS}
)

target_proto_addincls(yt-flow-ydb-sdk-api-grpc
  ${PROJECT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${YT_FLOW_YDB_SDK_PROTOBUF_PROTO_ROOT}
)

target_proto_plugin(yt-flow-ydb-sdk-api-grpc
  grpc_cpp
  ${YT_FLOW_YDB_SDK_GRPC_CPP_BIN}
)

if (NOT CMAKE_CROSSCOMPILING)
  add_dependencies(yt-flow-ydb-sdk-api-grpc
    ${YT_FLOW_YDB_SDK_GRPC_CPP_DEPENDENCY}
  )
endif()

set(YT_FLOW_YDB_SDK_PUBLIC_INCLUDES
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include
)
if (vanilla_protobuf STREQUAL "yes")
  file(GENERATE
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/vanilla_protobuf/ydb-cpp-sdk/type_switcher.h
    CONTENT "#pragma once\n\n#include <string>\n\nnamespace NYdb {\ninline namespace V3 {\n\nusing TStringType = std::string;\n\n}\n}\n"
  )
  list(PREPEND YT_FLOW_YDB_SDK_PUBLIC_INCLUDES
    ${CMAKE_CURRENT_BINARY_DIR}/vanilla_protobuf
  )

  set(YT_FLOW_YDB_SDK_STANDARD_FORMAT_PREINCLUDE
    ${CMAKE_CURRENT_BINARY_DIR}/vanilla_protobuf/standard_format_preinclude.h
  )
  file(GENERATE
    OUTPUT ${YT_FLOW_YDB_SDK_STANDARD_FORMAT_PREINCLUDE}
    CONTENT "#pragma once\n\n#if defined(__has_include)\n#if __has_include(<format>)\n#include <format>\n#endif\n#endif\n"
  )
  set(YT_FLOW_YDB_SDK_GTEST_HEADER_CONSUMER
    "$<IN_LIST:${PROJECT_SOURCE_DIR}/contrib/restricted/googletest/googletest/include,$<TARGET_PROPERTY:INCLUDE_DIRECTORIES>>"
  )
  set(YT_FLOW_YDB_SDK_STANDARD_FORMAT_DEPENDENCY_TARGET
    "$<OR:$<STREQUAL:$<TARGET_PROPERTY:NAME>,contrib-libs-re2>,$<STREQUAL:$<TARGET_PROPERTY:NAME>,contrib-restricted-abseil-cpp-tstring>,$<STREQUAL:$<TARGET_PROPERTY:NAME>,contrib-libs-grpc>,${YT_FLOW_YDB_SDK_GTEST_HEADER_CONSUMER}>"
  )
  set(YT_FLOW_YDB_SDK_STANDARD_FORMAT_DEPENDENCY_CXX_COMPILATION
    "$<AND:$<COMPILE_LANGUAGE:CXX>,${YT_FLOW_YDB_SDK_STANDARD_FORMAT_DEPENDENCY_TARGET}>"
  )
  if (YT_FLOW_YDB_SDK_MSVC_FRONTEND)
    set(YT_FLOW_YDB_SDK_STANDARD_FORMAT_PREINCLUDE_OPTION
      "/FI${YT_FLOW_YDB_SDK_STANDARD_FORMAT_PREINCLUDE}"
    )
  else()
    set(YT_FLOW_YDB_SDK_STANDARD_FORMAT_PREINCLUDE_OPTION
      "SHELL:-include \"${YT_FLOW_YDB_SDK_STANDARD_FORMAT_PREINCLUDE}\""
    )
  endif()
  set_property(DIRECTORY ${PROJECT_SOURCE_DIR} APPEND PROPERTY COMPILE_OPTIONS
    "$<${YT_FLOW_YDB_SDK_STANDARD_FORMAT_DEPENDENCY_CXX_COMPILATION}:${YT_FLOW_YDB_SDK_STANDARD_FORMAT_PREINCLUDE_OPTION}>"
  )
  set(YT_FLOW_YDB_SDK_OPEN_VCDIFF_CXX_COMPILATION
    "$<AND:$<COMPILE_LANGUAGE:CXX>,$<STREQUAL:$<TARGET_PROPERTY:NAME>,contrib-tools-open-vcdiff>>"
  )
  if (YT_FLOW_YDB_SDK_MSVC_FRONTEND)
    set(YT_FLOW_YDB_SDK_OPEN_VCDIFF_ALGORITHM_PREINCLUDE_OPTION "/FIalgorithm")
  else()
    set(YT_FLOW_YDB_SDK_OPEN_VCDIFF_ALGORITHM_PREINCLUDE_OPTION "SHELL:-include algorithm")
  endif()
  set_property(DIRECTORY ${PROJECT_SOURCE_DIR} APPEND PROPERTY COMPILE_OPTIONS
    "$<${YT_FLOW_YDB_SDK_OPEN_VCDIFF_CXX_COMPILATION}:${YT_FLOW_YDB_SDK_OPEN_VCDIFF_ALGORITHM_PREINCLUDE_OPTION}>"
  )
  set(YT_FLOW_YDB_SDK_WAVM_LIB_CXX_COMPILATION
    "$<AND:$<COMPILE_LANGUAGE:CXX>,$<STREQUAL:$<TARGET_PROPERTY:NAME>,restricted-wavm-Lib>>"
  )
  if (YT_FLOW_YDB_SDK_MSVC_FRONTEND)
    set(YT_FLOW_YDB_SDK_WAVM_LIB_OPTIONAL_PREINCLUDE_OPTION "/FIoptional")
  else()
    set(YT_FLOW_YDB_SDK_WAVM_LIB_OPTIONAL_PREINCLUDE_OPTION "SHELL:-include optional")
  endif()
  set_property(DIRECTORY ${PROJECT_SOURCE_DIR} APPEND PROPERTY COMPILE_OPTIONS
    "$<${YT_FLOW_YDB_SDK_WAVM_LIB_CXX_COMPILATION}:${YT_FLOW_YDB_SDK_WAVM_LIB_OPTIONAL_PREINCLUDE_OPTION}>"
  )

  set(YT_FLOW_YDB_SDK_PROTOBUF_NAMESPACE_ALIAS_PREINCLUDE
    ${CMAKE_CURRENT_BINARY_DIR}/vanilla_protobuf/protobuf_namespace_alias.h
  )
  file(GENERATE
    OUTPUT ${YT_FLOW_YDB_SDK_PROTOBUF_NAMESPACE_ALIAS_PREINCLUDE}
    CONTENT "#pragma once\n#include <google/protobuf/io/coded_stream.h>\nnamespace NProtoBuf = ::google::protobuf;\n"
  )
  set(YT_FLOW_YDB_SDK_FRAMING_CXX_COMPILATION
    "$<AND:$<COMPILE_LANGUAGE:CXX>,$<STREQUAL:$<TARGET_PROPERTY:NAME>,library-cpp-framing>>"
  )
  if (YT_FLOW_YDB_SDK_MSVC_FRONTEND)
    set(YT_FLOW_YDB_SDK_PROTOBUF_NAMESPACE_ALIAS_PREINCLUDE_OPTION
      "/FI${YT_FLOW_YDB_SDK_PROTOBUF_NAMESPACE_ALIAS_PREINCLUDE}"
    )
  else()
    set(YT_FLOW_YDB_SDK_PROTOBUF_NAMESPACE_ALIAS_PREINCLUDE_OPTION
      "SHELL:-include \"${YT_FLOW_YDB_SDK_PROTOBUF_NAMESPACE_ALIAS_PREINCLUDE}\""
    )
  endif()
  set_property(DIRECTORY ${PROJECT_SOURCE_DIR} APPEND PROPERTY COMPILE_OPTIONS
    "$<${YT_FLOW_YDB_SDK_FRAMING_CXX_COMPILATION}:${YT_FLOW_YDB_SDK_PROTOBUF_NAMESPACE_ALIAS_PREINCLUDE_OPTION}>"
  )
endif()

add_library(yt-flow-ydb-sdk STATIC)

target_compile_options(yt-flow-ydb-sdk PRIVATE
  -DNO_CUSTOM_CHAR_PTR_STD_COMPARATOR
)

target_include_directories(yt-flow-ydb-sdk PUBLIC
  ${YT_FLOW_YDB_SDK_PUBLIC_INCLUDES}
)

target_include_directories(yt-flow-ydb-sdk PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk
)

target_link_libraries(yt-flow-ydb-sdk PUBLIC
  yt-flow-ydb-sdk-api-grpc
)

target_sources(yt-flow-ydb-sdk PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/common_client/impl/client.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/common_client/settings.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/driver/driver.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl/federated_deferred_commit.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl/federated_read_session.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl/federated_read_session_event.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl/federated_topic.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl/federated_topic_impl.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl/federated_write_session.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/federated_topic/impl/federation_observer.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/endpoints/endpoints.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/executor/executor_impl.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/common/balancing_policies.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/common/client_pid.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/common/getenv.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/common/parser.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/db_driver_state/endpoint_pool.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/db_driver_state/state.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/grpc_connections/actions.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/grpc_connections/grpc_connections.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/logger/log.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/make_request/make.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/plain_status/status.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/retry/bulk_upsert_retry_state.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/retry/retry.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/scheme_helpers/helpers.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/sdk_runtime/runtime.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/thread_pool/pool.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/internal/value_helpers/helpers.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/observability/error_category/error_category.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/observability/metric_buffer.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/observability/metrics.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/observability/observation.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/observability/span.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/session/kqp_session_common.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/session/session_pool.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/impl/stats/stats.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/metrics/metrics.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/params/impl.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/params/params.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/proto/accessor.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/query/client.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/query/impl/client_session.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/query/impl/exec_query.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/query/impl/session_state_handler.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/query/query.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/query/stats.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/query/tx.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/resources/ydb_ca.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/resources/ydb_resources.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/result/out.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/result/proto_accessor.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/result/result.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/scheme/out.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/scheme/scheme.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/impl/client_session.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/impl/data_query.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/impl/readers.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/impl/request_migrator.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/impl/table_client.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/impl/transaction.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/out.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/proto_accessor.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/query_stats/stats.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/table/table.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/common/executor_impl.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/common/retry_policy.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/common.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/deferred_commit.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/deferred_publication_ack_tracker.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/deferred_publications.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/direct_reader.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/event_handlers.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/offsets_collector.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/proto_accessor.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/read_session.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/read_session_event.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/topic.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/topic_impl.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/transaction.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/write_session_impl.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/out.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/trace/trace.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/core_facility/simple_core_facility.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/credentials/credentials.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/credentials/login/login.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/exceptions/exceptions.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/executor/executor.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/fatal_error_handlers/handlers.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/operation/operation.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/operation/out.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/status/status.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/virtual_timestamp.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/ydb.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/value/out.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/value/value.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/decimal/yql_decimal.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/grpc/client/grpc_common.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/issue/utf8.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/issue/yql_issue.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/issue/yql_issue_message.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/operation_id/operation_id.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/persqueue/obfuscate/obfuscate.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/string_utils/helpers/helpers.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/time/time.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/uuid/uuid.cpp
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/sdk/cpp/src/library/kafka/kafka_messages_int.cpp
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/sdk/cpp/src/library/kafka/kafka_records.cpp
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/sdk/cpp/src/library/kafka/kafka_write_buffer.cpp
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/sdk/cpp/src/library/decimal/yql_decimal.cpp
)

set(YT_FLOW_YDB_SDK_STANDARD_FORMAT_SOURCES
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/producer.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/impl/write_session.cpp
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/library/grpc/client/grpc_client_low.cpp
)

if (vanilla_protobuf STREQUAL "yes")
  target_compile_definitions(yt-flow-ydb-sdk PRIVATE
    YDB_SDK_OSS
  )
  target_sources(yt-flow-ydb-sdk PRIVATE
    ${PROJECT_SOURCE_DIR}/yt/yt/flow/extensions/ydb/sdk/vanilla_protobuf_out.cpp
  )

  include(CheckCXXSourceCompiles)
  function(yt_flow_ydb_sdk_check_standard_format configuration)
    set(CMAKE_TRY_COMPILE_CONFIGURATION "${configuration}")
    unset(YT_FLOW_YDB_SDK_HAS_STANDARD_FORMAT)
    unset(YT_FLOW_YDB_SDK_HAS_STANDARD_FORMAT CACHE)
    check_cxx_source_compiles(
      "#include <cstdint>\n#include <format>\n#include <string>\nint main() {\n  return std::format(\"{}_{}\", std::string(\"a\"), std::uint32_t(1)).size() == 3 ? 0 : 1;\n}\n"
      YT_FLOW_YDB_SDK_HAS_STANDARD_FORMAT
    )
    set(YT_FLOW_YDB_SDK_HAS_STANDARD_FORMAT "${YT_FLOW_YDB_SDK_HAS_STANDARD_FORMAT}" PARENT_SCOPE)
  endfunction()
  if (CMAKE_CONFIGURATION_TYPES)
    foreach (YT_FLOW_YDB_SDK_FORMAT_CONFIGURATION IN LISTS CMAKE_CONFIGURATION_TYPES)
      yt_flow_ydb_sdk_check_standard_format("${YT_FLOW_YDB_SDK_FORMAT_CONFIGURATION}")
      if (NOT YT_FLOW_YDB_SDK_HAS_STANDARD_FORMAT)
        break()
      endif()
    endforeach()
  else()
    yt_flow_ydb_sdk_check_standard_format("${CMAKE_BUILD_TYPE}")
  endif()
  if (NOT YT_FLOW_YDB_SDK_HAS_STANDARD_FORMAT)
    set(YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCES)
    foreach (YT_FLOW_YDB_SDK_CANONICAL_SOURCE IN LISTS YT_FLOW_YDB_SDK_STANDARD_FORMAT_SOURCES)
      file(RELATIVE_PATH YT_FLOW_YDB_SDK_RELATIVE_SOURCE ${PROJECT_SOURCE_DIR} ${YT_FLOW_YDB_SDK_CANONICAL_SOURCE})
      set(YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCE ${CMAKE_CURRENT_BINARY_DIR}/fmt_format/${YT_FLOW_YDB_SDK_RELATIVE_SOURCE})
      file(READ ${YT_FLOW_YDB_SDK_CANONICAL_SOURCE} YT_FLOW_YDB_SDK_SOURCE_CONTENT)
      string(REPLACE "#include <format>" "#include <fmt/format.h>" YT_FLOW_YDB_SDK_SOURCE_CONTENT "${YT_FLOW_YDB_SDK_SOURCE_CONTENT}")
      string(REPLACE "std::format(" "fmt::format(" YT_FLOW_YDB_SDK_SOURCE_CONTENT "${YT_FLOW_YDB_SDK_SOURCE_CONTENT}")
      set(YT_FLOW_YDB_SDK_SOURCE_CONTENT "#line 1 \"${YT_FLOW_YDB_SDK_CANONICAL_SOURCE}\"\n${YT_FLOW_YDB_SDK_SOURCE_CONTENT}")
      set(YT_FLOW_YDB_SDK_PREVIOUS_SOURCE_CONTENT)
      if (EXISTS ${YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCE})
        file(READ ${YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCE} YT_FLOW_YDB_SDK_PREVIOUS_SOURCE_CONTENT)
      endif()
      if (NOT YT_FLOW_YDB_SDK_PREVIOUS_SOURCE_CONTENT STREQUAL YT_FLOW_YDB_SDK_SOURCE_CONTENT)
        file(WRITE ${YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCE} "${YT_FLOW_YDB_SDK_SOURCE_CONTENT}")
      endif()
      get_filename_component(YT_FLOW_YDB_SDK_CANONICAL_SOURCE_DIRECTORY ${YT_FLOW_YDB_SDK_CANONICAL_SOURCE} DIRECTORY)
      set_source_files_properties(${YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCE} PROPERTIES
        INCLUDE_DIRECTORIES ${YT_FLOW_YDB_SDK_CANONICAL_SOURCE_DIRECTORY}
      )
      set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${YT_FLOW_YDB_SDK_CANONICAL_SOURCE})
      list(APPEND YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCES ${YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCE})
    endforeach()
    set(YT_FLOW_YDB_SDK_STANDARD_FORMAT_SOURCES ${YT_FLOW_YDB_SDK_FMT_FORMAT_SOURCES})
    target_link_libraries(yt-flow-ydb-sdk PRIVATE
      contrib-libs-fmt
    )
  endif()
endif()

target_sources(yt-flow-ydb-sdk PRIVATE
  ${YT_FLOW_YDB_SDK_STANDARD_FORMAT_SOURCES}
)

if (YT_FLOW_YDB_SDK_MSVC_FRONTEND)
  set(YT_FLOW_YDB_SDK_STR_PREINCLUDE_OPTIONS "/FIutil/stream/str.h")
else()
  set(YT_FLOW_YDB_SDK_STR_PREINCLUDE_OPTIONS "-include;util/stream/str.h")
endif()
set_source_files_properties(
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/types/virtual_timestamp.cpp
  PROPERTIES COMPILE_OPTIONS "${YT_FLOW_YDB_SDK_STR_PREINCLUDE_OPTIONS}"
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/codecs.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/codecs.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/control_plane.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/control_plane.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/read_events.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/read_events.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/write_events.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/topic/write_events.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/federated_topic/federated_topic.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/result/result.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/result/result.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/scheme/scheme.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/scheme/scheme.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/table/table_enum.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/table/table_enum.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/s3_settings.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/s3_settings.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/status_codes.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/status_codes.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/ydb.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/types/ydb.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/value/value.h
  INCLUDE_HEADERS
  contrib/libs/ydb-cpp-sdk/include/ydb-cpp-sdk/client/value/value.h
)

generate_enum_serilization(yt-flow-ydb-sdk
  ${PROJECT_SOURCE_DIR}/contrib/ydb/public/sdk/cpp/src/library/kafka/kafka.h
  INCLUDE_HEADERS
  contrib/ydb/public/sdk/cpp/src/library/kafka/kafka.h
)

add_global_library_for(yt-flow-ydb-sdk.global
  yt-flow-ydb-sdk
)

target_compile_options(yt-flow-ydb-sdk.global PRIVATE
  -DNO_CUSTOM_CHAR_PTR_STD_COMPARATOR
)

target_include_directories(yt-flow-ydb-sdk.global PUBLIC
  ${YT_FLOW_YDB_SDK_PUBLIC_INCLUDES}
)

target_include_directories(yt-flow-ydb-sdk.global PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk
)

target_link_libraries(yt-flow-ydb-sdk.global PUBLIC
  yt-flow-ydb-sdk-api-grpc
)

target_sources(yt-flow-ydb-sdk.global PRIVATE
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/topic/codecs/codecs.cpp
  ${CMAKE_CURRENT_BINARY_DIR}/ydb_root_ca_resource.cpp
)

resources(yt-flow-ydb-sdk.global
  ${CMAKE_CURRENT_BINARY_DIR}/ydb_root_ca_resource.cpp
  INPUTS
  ${PROJECT_SOURCE_DIR}/contrib/libs/ydb-cpp-sdk/src/client/resources/ydb_root_ca.pem
  KEYS
  ydb_root_ca_v3.pem
)

target_link_libraries(${YT_FLOW_YDB_SDK_ADAPTER} INTERFACE
  yt-flow-ydb-sdk
)

function(yt_flow_ydb_sdk_link_static_idn)
  if (NOT TARGET library-cpp-uri OR NOT TARGET libs-libidn-static)
    return()
  endif()
  foreach (YT_FLOW_YDB_SDK_URI_LINK_PROPERTY LINK_LIBRARIES INTERFACE_LINK_LIBRARIES)
    get_target_property(YT_FLOW_YDB_SDK_URI_LINKS library-cpp-uri ${YT_FLOW_YDB_SDK_URI_LINK_PROPERTY})
    if (YT_FLOW_YDB_SDK_URI_LINKS)
      list(TRANSFORM YT_FLOW_YDB_SDK_URI_LINKS REPLACE "^IDN::IDN$" libs-libidn-static)
      set_target_properties(library-cpp-uri PROPERTIES
        ${YT_FLOW_YDB_SDK_URI_LINK_PROPERTY} "${YT_FLOW_YDB_SDK_URI_LINKS}"
      )
    endif()
  endforeach()
endfunction()

if (CMAKE_VERSION VERSION_LESS 3.19)
  yt_flow_ydb_sdk_link_static_idn()
else()
  cmake_language(DEFER DIRECTORY ${CMAKE_SOURCE_DIR} CALL yt_flow_ydb_sdk_link_static_idn)
endif()
