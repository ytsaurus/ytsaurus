set( DIRS
  actions
  election
  cell_master
  cell_node
  cell_scheduler
  chunk_holder
#  chunk_client
  chunk_server
  transaction_server
  transaction_client
  logging
  meta_state
  misc
  rpc
  bus
  ytree
  cypress
  orchid
  file_server
#  file_client
  table_server
#  table_client
  monitoring
  profiling
#  driver
  object_server
  scheduler
  exec_agent
#  job_proxy
)

foreach( _d_ ${DIRS} )
  include( ${_d_}/CMakeLists.txt )
  source_group( ${_d_} REGULAR_EXPRESSION "${_d_}/.*\\.((cpp)|(h)|(proto))")
endforeach()

add_msvc_precompiled_header( "stdafx.h" "stdafx.cpp" YT_SOURCE_FILES )
add_library( ytlib STATIC
  ${YT_SOURCE_FILES}
)

target_link_libraries ( ytlib
  ytext-arcadia-util

  ytext-quality-misc
  ytext-quality-netliba_v6
  ytext-json

  # TODO(sandello): arcadia-utils should depend on these targets.
  ytext-snappy
  ytext-lz4
  ytext-fastlz
  ytext-minilzo
  ytext-zlib
  protobuf
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_SOURCE_DIR}/contrib/tclap

  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)
