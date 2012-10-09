set( DIRS
  bootstrap
  cell_master
  cell_node
  cell_scheduler
  chunk_holder
  chunk_server
  transaction_server
  cypress_server
  file_server
  table_server
  object_server
  scheduler
  exec_agent
  job_proxy
  orchid
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

add_executable( ytserver
  "main.cpp"
  ${YT_SOURCE_FILES}
)

include_directories(
  ${CMAKE_SOURCE_DIR}/contrib/tclap
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)

target_link_libraries( ytserver
  ytlib
)

install(
  TARGETS ytserver
  RUNTIME DESTINATION bin
)
