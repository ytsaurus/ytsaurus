include_directories(
  ../ytlib
)

set( SRCS
  utmain.cpp

  change_log_ut.cpp
  async_result_ut.cpp
  snapshot_ut.cpp
  preprocessor_ut.cpp
  ref_counted_tracker_ut.cpp
  pattern_formatter_ut.cpp
  enum_ut.cpp
  yverify_ut.cpp
  metric_ut.cpp
  meta_state_map_ut.cpp
  foreach_ut.cpp
  thread_affinity_ut.cpp
  ytree_fluent_ut.cpp
  yson_parser_ut.cpp
  yson_writer_ut.cpp
  zigzag_ut.cpp
  varint_ut.cpp
  async_change_log_ut.cpp
  intrusive_ptr_ut.cpp
  nullable_ut.cpp
  tree_builder_ut.cpp
  configurable_ut.cpp
  bus_ut.cpp
  serialize_ut.cpp
  schema_ut.cpp
  rpc_ut.cpp
  ypath_ut.cpp
  mpl_ut.cpp
  weak_ptr_ut.cpp
  callback_ut.cpp
  bind_ut.cpp
  table_client_ut.cpp
  lexer_ut.cpp
  guid_ut.cpp
  small_vector_ut.cpp
  small_string_ut.cpp
  url_ut.cpp
)

protoc( rpc_ut.proto PROTO_SRC )

add_msvc_precompiled_header( "stdafx.h" "stdafx.cpp" SRCS )
add_executable( unittester
  ${CMAKE_SOURCE_DIR}/contrib/testing/framework.cpp
  ${SRCS}
  ${PROTO_SRC}
)

target_link_libraries( unittester
  ytlib
)

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( unittester stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( unittester PROPERTIES LINK_FLAGS "-nodefaultlibs -L${CMAKE_BINARY_DIR}/lib" )
  endif()
endif()
