set( DIRS
  misc
  ypath
  election
  orchid
  driver
  query_client
  scheduler
  chunk_client
  cypress_client
  file_client
  table_client
  object_client
  security_client
  node_tracker_client
  job_tracker_client
  job_prober_client
  transaction_client
  tablet_client
  monitoring
  cgroup
  hydra
  hive
  formats
  api
  journal_client
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
  stdafx.h
  stdafx.cpp
  YT_SOURCE_FILES
)

add_library( ytlib STATIC
  ${YT_SOURCE_FILES}
  shutdown.cpp
)

target_link_libraries ( ytlib
  ytcore
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
)
