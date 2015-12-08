set( SRCS
<<<<<<< HEAD
  ${CMAKE_CURRENT_LIST_DIR}/plan_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/coordinator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/key_trie.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ast.cpp
  ${CMAKE_CURRENT_LIST_DIR}/plan_fragment.cpp
  ${CMAKE_CURRENT_LIST_DIR}/plan_fragment_common.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_preparer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/query_statistics.cpp
  ${CMAKE_CURRENT_LIST_DIR}/evaluator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/column_evaluator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/range_inferrer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/function_context.cpp
)

set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/callbacks.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/plan_helpers.h
  ${CMAKE_CURRENT_LIST_DIR}/coordinator.h
  ${CMAKE_CURRENT_LIST_DIR}/lexer.h
  ${CMAKE_CURRENT_LIST_DIR}/parser.hpp
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/query_service_proxy.h
  ${CMAKE_CURRENT_LIST_DIR}/key_trie.h
  ${CMAKE_CURRENT_LIST_DIR}/ast.h
  ${CMAKE_CURRENT_LIST_DIR}/plan_fragment.h
  ${CMAKE_CURRENT_LIST_DIR}/plan_fragment_common.h
  ${CMAKE_CURRENT_LIST_DIR}/query_preparer.h
  ${CMAKE_CURRENT_LIST_DIR}/query_statistics.h
  ${CMAKE_CURRENT_LIST_DIR}/evaluator.h
  ${CMAKE_CURRENT_LIST_DIR}/column_evaluator.h
  ${CMAKE_CURRENT_LIST_DIR}/range_inferrer.h
  ${CMAKE_CURRENT_LIST_DIR}/function_context.h
)

if (YT_BUILD_ENABLE_LLVM)
  set( SRCS
    ${SRCS}
    ${CMAKE_CURRENT_LIST_DIR}/cg_routines.cpp
    ${CMAKE_CURRENT_LIST_DIR}/cg_ir_builder.cpp
    ${CMAKE_CURRENT_LIST_DIR}/cg_helpers.cpp
    ${CMAKE_CURRENT_LIST_DIR}/cg_fragment_compiler.cpp
    ${CMAKE_CURRENT_LIST_DIR}/folding_profiler.cpp
    ${CMAKE_CURRENT_LIST_DIR}/evaluation_helpers.cpp
    ${CMAKE_CURRENT_LIST_DIR}/functions.cpp
    ${CMAKE_CURRENT_LIST_DIR}/builtin_functions.cpp
    ${CMAKE_CURRENT_LIST_DIR}/user_defined_functions.cpp
    ${CMAKE_CURRENT_LIST_DIR}/function_registry.cpp
    ${CMAKE_CURRENT_LIST_DIR}/udf_descriptor.cpp
  )

  set( HDRS
    ${HDRS}
    ${CMAKE_CURRENT_LIST_DIR}/cg_types.h
    ${CMAKE_CURRENT_LIST_DIR}/cg_routines.h
    ${CMAKE_CURRENT_LIST_DIR}/cg_ir_builder.h
    ${CMAKE_CURRENT_LIST_DIR}/cg_helpers.h
    ${CMAKE_CURRENT_LIST_DIR}/cg_fragment_compiler.h
    ${CMAKE_CURRENT_LIST_DIR}/folding_profiler.h
    ${CMAKE_CURRENT_LIST_DIR}/evaluation_helpers.h
    ${CMAKE_CURRENT_LIST_DIR}/functions.h
    ${CMAKE_CURRENT_LIST_DIR}/builtin_functions.h
    ${CMAKE_CURRENT_LIST_DIR}/user_defined_functions.h
    ${CMAKE_CURRENT_LIST_DIR}/function_registry.h
    ${CMAKE_CURRENT_LIST_DIR}/udf_descriptor.h
  )
endif()

=======
  ${BASE}/ast.cpp
  ${BASE}/builtin_functions.cpp
  ${BASE}/cg_fragment_compiler.cpp
  ${BASE}/cg_helpers.cpp
  ${BASE}/cg_ir_builder.cpp
  ${BASE}/cg_routines.cpp
  ${BASE}/column_evaluator.cpp
  ${BASE}/coordinator.cpp
  ${BASE}/evaluation_helpers.cpp
  ${BASE}/evaluator.cpp
  ${BASE}/folding_profiler.cpp
  ${BASE}/function_context.cpp
  ${BASE}/function_registry.cpp
  ${BASE}/functions.cpp
  ${BASE}/helpers.cpp
  ${BASE}/key_trie.cpp
  ${BASE}/plan_fragment_common.cpp
  ${BASE}/plan_fragment.cpp
  ${BASE}/plan_helpers.cpp
  ${BASE}/private.cpp
  ${BASE}/query_preparer.cpp
  ${BASE}/query_statistics.cpp  
  ${BASE}/range_inferrer.cpp
  ${BASE}/udf_descriptor.cpp
  ${BASE}/user_defined_functions.cpp
)

set( HDRS
  ${BASE}/ast.h
  ${BASE}/builtin_functions.h
  ${BASE}/callbacks.h
  ${BASE}/cg_fragment_compiler.h
  ${BASE}/cg_helpers.h
  ${BASE}/cg_ir_builder.h
  ${BASE}/cg_routines.h
  ${BASE}/cg_types.h
  ${BASE}/column_evaluator.h
  ${BASE}/config.h
  ${BASE}/coordinator.h
  ${BASE}/evaluation_helpers.h
  ${BASE}/evaluator.h
  ${BASE}/folding_profiler.h
  ${BASE}/function_context.h
  ${BASE}/function_registry.h
  ${BASE}/functions.h
  ${BASE}/helpers.h
  ${BASE}/key_trie.h
  ${BASE}/lexer.h
  ${BASE}/parser.hpp
  ${BASE}/plan_fragment_common.h
  ${BASE}/plan_fragment.h
  ${BASE}/plan_helpers.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/query_preparer.h
  ${BASE}/query_service_proxy.h
  ${BASE}/query_statistics.h
  ${BASE}/range_inferrer.h
  ${BASE}/udf_descriptor.h
  ${BASE}/user_defined_functions.h
)

>>>>>>> origin/prestable/0.17.4
list( APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

<<<<<<< HEAD
include_directories(
  ${CMAKE_SOURCE_DIR}/contrib/libs/re2
)

bison( ${CMAKE_CURRENT_LIST_DIR}/parser.yy YT_SOURCE_FILES )
ragel( ${CMAKE_CURRENT_LIST_DIR}/lexer.rl YT_SOURCE_FILES )
=======
bison( ${BASE}/parser.yy YT_SOURCE_FILES )
ragel( ${BASE}/lexer.rl YT_SOURCE_FILES )
>>>>>>> origin/prestable/0.17.4

protoc( ${CMAKE_CURRENT_LIST_DIR}/query_statistics.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/plan_fragment.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/query_service.proto YT_SOURCE_FILES )

udf_bc(
  ${CMAKE_CURRENT_LIST_DIR}/udf/hyperloglog.cpp
  YT_SOURCE_FILES
  SYMBOLS
    cardinality_init
    cardinality_update
    cardinality_merge
    cardinality_finalize
  FILES
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/unversioned_value.cpp
    ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhash.cc
)
udf_bc(
  ${CMAKE_CURRENT_LIST_DIR}/udf/farm_hash.cpp
  YT_SOURCE_FILES
  FILES
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/unversioned_value.cpp
    ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhash.cc
)
udf_bc(
  ${CMAKE_CURRENT_LIST_DIR}/udf/regex.cpp
  YT_SOURCE_FILES
  FILES
    ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client/function_context.cpp
  SYMBOLS
    regex_full_match
    regex_partial_match
    regex_replace_first
    regex_replace_all
    regex_extract
    regex_escape
)
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/double_cast.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/int64.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/is_null.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/is_substr.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/is_prefix.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/lower.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/sleep.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/uint64.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/min.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/max.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/sum.c YT_SOURCE_FILES )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/avg.c YT_SOURCE_FILES )
