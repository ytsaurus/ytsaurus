PROTOC( rpc_ut.proto PROTO_SRC )

include_directories(
  ../ytlib
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/include
)

add_executable( unittester
    ${CMAKE_SOURCE_DIR}/contrib/testing/framework.cpp
    stdafx.cpp
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
    yson_reader_ut.cpp
    yson_writer_ut.cpp
    zigzag_ut.cpp
    varint_ut.cpp
    async_change_log_ut.cpp
    intrusive_ptr_ut.cpp
    tree_builder_ut.cpp
    config_ut.cpp
    bus_ut.cpp

    rpc_ut.cpp
    ${PROTO_SRC}
)

if (CMAKE_COMPILER_IS_GNUCXX)
  SET( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -nodefaultlibs -L${CMAKE_BINARY_DIR}/lib -Wl,-rpath=${CMAKE_BINARY_DIR}/lib" )

  target_link_libraries( unittester
    -lytlib
    -larcadia-utils
    -lsnappy
    -llz4
    -lfastlz
    -lminilzo
    -larczlib
    -lprotobuf
    -lqmisc
    -lNetLiba
    -lyajl
    -ljson
   
    -lstlport
  )

  target_link_libraries( unittester -larcadia-utils )

  target_link_libraries( unittester -ldl )

  target_link_libraries( unittester -lc -lm -lpthread -ldl -lgcc_s )
endif()
