bison( ${CMAKE_CURRENT_LIST_DIR}/parser.yy YT_SOURCE_FILES )
ragel( ${CMAKE_CURRENT_LIST_DIR}/lexer.rl YT_SOURCE_FILES )

protoc( ${CMAKE_CURRENT_LIST_DIR}/query.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/functions_cache.proto YT_SOURCE_FILES )
protoc( ${CMAKE_CURRENT_LIST_DIR}/query_service.proto YT_SOURCE_FILES )

udf_bc_header(
  ${CMAKE_CURRENT_LIST_DIR}/udf/hyperloglog.cpp
  YT_SOURCE_FILES
  SYMBOLS
    cardinality_init
    cardinality_update
    cardinality_merge
    cardinality_finalize
  FILES
    ${CMAKE_SOURCE_DIR}/yt/client/table_client/unversioned_value.cpp
    ${CMAKE_SOURCE_DIR}/contrib/libs/farmhash/farmhash.cc
)
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/farm_hash.cpp YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/bigb_hash.cpp YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/make_map.cpp YT_SOURCE_FILES )

udf_bc_header(
  ${CMAKE_CURRENT_LIST_DIR}/udf/regex.cpp
  YT_SOURCE_FILES
  SYMBOLS
    regex_full_match
    regex_partial_match
    regex_replace_first
    regex_replace_all
    regex_extract
    regex_escape
)
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/avg.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/concat.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/first.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/format_guid.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/is_prefix.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/is_substr.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/max.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/min.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/sleep.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/sum.c YT_SOURCE_FILES )
udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/to_any.cpp YT_SOURCE_FILES )

udf_bc_header(
  ${CMAKE_CURRENT_LIST_DIR}/udf/ypath_get.c
  YT_SOURCE_FILES
  SYMBOLS
    try_get_int64
    get_int64
    try_get_uint64
    get_uint64
    try_get_double
    get_double
    try_get_boolean
    get_boolean
    try_get_string
    get_string
    try_get_any
    get_any
)

udf_bc_header( ${CMAKE_CURRENT_LIST_DIR}/udf/lower.cpp YT_SOURCE_FILES )

udf_bc_header(
  ${CMAKE_CURRENT_LIST_DIR}/udf/dates.c
  YT_SOURCE_FILES
  SYMBOLS
    format_timestamp
    timestamp_floor_hour
    timestamp_floor_day
    timestamp_floor_week
    timestamp_floor_month
    timestamp_floor_year
)

udf_bc_header(
  ${CMAKE_CURRENT_LIST_DIR}/udf/list_contains.cpp
  YT_SOURCE_FILES
  SYMBOLS
    list_contains
)

udf_bc_header(
  ${CMAKE_CURRENT_LIST_DIR}/udf/any_to_yson_string.cpp
  YT_SOURCE_FILES
  SYMBOLS
    any_to_yson_string
)
