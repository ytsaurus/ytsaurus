set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client )

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
    ${CMAKE_SOURCE_DIR}/yt/contrib/farmhash/farmhash.cc
)
udf_bc(
  ${BASE}/udf/farm_hash.cpp
  YT_SOURCE_FILES
  FILES
    ${CMAKE_SOURCE_DIR}/yt/ytlib/table_client/unversioned_value.cpp
    ${CMAKE_SOURCE_DIR}/yt/contrib/farmhash/farmhash.cc
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
udf_bc( ${BASE}/udf/concat.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/sleep.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/uint64.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/min.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/max.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/sum.c YT_SOURCE_FILES )
udf_bc( ${BASE}/udf/avg.c YT_SOURCE_FILES )

udf_bc(
  ${BASE}/udf/lower.cpp
  YT_SOURCE_FILES
  FILES
    ${CMAKE_SOURCE_DIR}/util/charset/utf.cpp
    ${CMAKE_SOURCE_DIR}/util/charset/generated/unidata.cpp)
