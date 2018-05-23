include(Sources.txt)

resolve_srcs(YT_SRCS YT_SRCS_RESOLVED)

udf_bc_header(${CMAKE_CURRENT_LIST_DIR}/udf/malloc_udf.c YT_SRCS_RESOLVED)
udf_bc_header(${CMAKE_CURRENT_LIST_DIR}/udf/test_udfs.c
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

udf_bc_header(${CMAKE_CURRENT_LIST_DIR}/udf/test_udfs_fc.cpp
  YT_SRCS_RESOLVED
  SYMBOLS
    udf_with_function_context
)

list(APPEND YT_SRCS_RESOLVED ${YT_SOURCE_FILES})

set_source_files_properties(${YT_SRCS_RESOLVED} PROPERTIES COMPILE_FLAGS -Wno-unused-result)

add_executable(unittester-ytlib
  ${YT_SRCS_RESOLVED}
)

target_link_libraries(unittester-ytlib
  yt-core-test-framework
  yt-library-client
  yt-library-client-auth
)
