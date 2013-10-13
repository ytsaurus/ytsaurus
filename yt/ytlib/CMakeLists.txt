set( DIRS
  ypath
  election
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
  hydra
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

if (HAVE_LIBUNWIND)
  target_link_libraries( ytlib unwind )
endif()

if (YT_BUILD_ENABLE_PYTHON)
  add_library( ytlib-without-lfalloc STATIC
      ${YT_SOURCE_FILES}
  )
  target_link_libraries ( ytlib-without-lfalloc
    ytcore-without-lfalloc
  )
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_SOURCE_DIR}/contrib/tclap
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)
