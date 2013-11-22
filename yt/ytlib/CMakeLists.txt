set( DIRS
  actions
  election
  compression
  erasure
  logging
  meta_state
  misc
  rpc
  bus
  ytree
  yson
  ypath
  orchid
  monitoring
  profiling
  driver
  formats
  scheduler
  chunk_client
  cypress_client
  file_client
  table_client
  object_client
  security_client
  fibers
  node_tracker_client
  job_tracker_client
  transaction_client
)

foreach( _d_ ${DIRS} )
  include( ${_d_}/CMakeLists.txt )
  source_group( ${_d_} REGULAR_EXPRESSION "${_d_}/.*\\.((cpp)|(h)|(proto))")
endforeach()

add_msvc_precompiled_header(
  "stdafx.h"
  "stdafx.cpp"
  YT_SOURCE_FILES
)

add_library( ytlib-no-lfalloc STATIC
  ${YT_SOURCE_FILES}
)

target_link_libraries ( ytlib-no-lfalloc
  ytext-arcadia-util
  ytext-coro
  ytext-ev
  ytext-jerasure
  ytext-json
  ytext-httpserver
  protobuf
)

add_library( ytlib STATIC
)

target_link_libraries( ytlib
  ytlib-no-lfalloc
  ytext-lfalloc
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_SOURCE_DIR}/contrib/tclap
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)

