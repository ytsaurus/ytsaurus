include_directories(
  ../ytlib
)
set( BASE ${CMAKE_SOURCE_DIR}/yt/unittests )
set( SRCS
  ${BASE}/attributes_ut.cpp
  ${BASE}/async_change_log_ut.cpp
  ${BASE}/async_result_ut.cpp
  ${BASE}/bind_ut.cpp
  ${BASE}/bus_ut.cpp
  ${BASE}/callback_ut.cpp
  ${BASE}/codec_ut.cpp
  ${BASE}/change_log_ut.cpp
  ${BASE}/configurable_ut.cpp
  ${BASE}/dsv_parser_ut.cpp
  ${BASE}/dsv_writer_ut.cpp
  ${BASE}/enum_ut.cpp
  ${BASE}/foreach_ut.cpp
  ${BASE}/guid_ut.cpp
  ${BASE}/intrusive_ptr_ut.cpp
  ${BASE}/json_parser_ut.cpp
  ${BASE}/json_writer_ut.cpp
  ${BASE}/lexer_ut.cpp
  ${BASE}/meta_state_map_ut.cpp
  ${BASE}/metric_ut.cpp
  ${BASE}/mpl_ut.cpp
  ${BASE}/nullable_ut.cpp
  ${BASE}/pattern_formatter_ut.cpp
  ${BASE}/preprocessor_ut.cpp
  ${BASE}/random_ut.cpp
  ${BASE}/ref_counted_tracker_ut.cpp
  ${BASE}/rpc_ut.cpp
  ${BASE}/schema_ut.cpp
  ${BASE}/small_string_ut.cpp
  ${BASE}/small_vector_ut.cpp
  ${BASE}/snapshot_ut.cpp
  ${BASE}/serialize_ut.cpp
#  ${BASE}/table_client_ut.cpp
  ${BASE}/thread_affinity_ut.cpp
  ${BASE}/tree_builder_ut.cpp
  ${BASE}/url_ut.cpp
  ${BASE}/utmain.cpp
  ${BASE}/varint_ut.cpp
  ${BASE}/weak_ptr_ut.cpp
  ${BASE}/yamr_parser_ut.cpp
  ${BASE}/yamr_writer_ut.cpp
  ${BASE}/ypath_ut.cpp
  ${BASE}/yson_ut.cpp
  ${BASE}/yson_parser_ut.cpp
  ${BASE}/yson_writer_ut.cpp
  ${BASE}/ytree_fluent_ut.cpp
  ${BASE}/yverify_ut.cpp
  ${BASE}/zigzag_ut.cpp
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
