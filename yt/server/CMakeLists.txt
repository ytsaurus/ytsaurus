set( DIRS
  misc
  cell_master
  cell_node
  cell_scheduler
  chunk_holder
  scheduler
  exec_agent
  job_agent
  job_proxy
  orchid
  chunk_server
  transaction_server
  cypress_server
  file_server
  table_server
  object_server
  security_server
  node_tracker_server
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

add_library( ytserver_lib
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

target_link_libraries( ytserver_lib
  ytlib
)

install(
  TARGETS ytserver
  RUNTIME DESTINATION bin
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE SETUID
)
