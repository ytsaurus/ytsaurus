set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client )
set( SRCS
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

list( APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

bison( ${BASE}/parser.yy YT_SOURCE_FILES )
ragel( ${BASE}/lexer.rl YT_SOURCE_FILES )

protoc( ${BASE}/query_statistics.proto YT_SOURCE_FILES )
protoc( ${BASE}/plan_fragment.proto YT_SOURCE_FILES )
protoc( ${BASE}/query_service.proto YT_SOURCE_FILES )

udf_bc(
  ${BASE}/udf/hyperloglog.cpp
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
  ${BASE}/udf/farm_hash.cpp
  YT_SOURCE_FILES
  FILES
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/unversioned_value.cpp
    ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhash.cc
)
udf_bc(
  ${BASE}/udf/regex.cpp
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
udf_bc( ${BASE}/udf/double_cast.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/int64.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/is_null.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/is_substr.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/is_prefix.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/lower.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/sleep.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/uint64.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/min.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/max.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/sum.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/avg.c YT_SOURCE_FILES )
