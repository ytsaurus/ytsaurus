set( DIRS
  ypath
  cell_directory
  election
  meta_state
  orchid
  driver
  scheduler
  chunk_client
  cypress_client
  file_client
  table_client
  object_client
  security_client
  node_tracker_client
  job_tracker_client
  transaction_client
  monitoring
)

set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib )
set( SRCS
  ${BASE}/shutdown.cpp
)
set( HDRS
  ${BASE}/shutdown.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
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

add_library( ytlib STATIC
  ${YT_SOURCE_FILES}
)

target_link_libraries ( ytlib
  ytcore
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_SOURCE_DIR}/contrib/tclap
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)

