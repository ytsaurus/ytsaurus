SET(PROTO_NAMESPACE yt/${YT_ABI})

include(Sources.txt)

set(YT_SOURCE_FILES)

list(APPEND YT_SRCS
  ${CMAKE_CURRENT_LIST_DIR}/cgroup_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_computed_columns_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_expressions_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_helpers.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_query_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/ql_range_inference_ut.cpp
  ${CMAKE_CURRENT_LIST_DIR}/value_consumer_ut.cpp
)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/malloc_udf.c YT_SRCS_RESOLVED )
udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/test_udfs.c
  YT_SRCS_RESOLVED
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

udf_bc( ${CMAKE_CURRENT_LIST_DIR}/udf/test_udfs_fc.cpp
  YT_SRCS_RESOLVED
  SYMBOLS
    udf_with_function_context
)

list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

if(MSVC)
  add_definitions(-D_VARIADIC_MAX=10)
  add_definitions(-DGTEST_HAS_SEH=0)
endif()

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(unittester
  ${YT_UNITTESTS_MAIN}
  ${YT_SRCS_RESOLVED}
)

target_link_libraries(unittester
  yt-core-test-framework
  yt-library-client
  yt-library-server
)
