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

if (YT_BUILD_YTLIB_SHARED)
  add_library( ytlib SHARED
    ${YT_SOURCE_FILES}
  )
  add_library( ytlib-without-lfalloc SHARED
    ${YT_SOURCE_FILES}
  )
  install(
    TARGETS ytlib
    LIBRARY DESTINATION lib COMPONENT libraries
  )
  install(
    TARGETS ytlib-without-lfalloc
    LIBRARY DESTINATION lib COMPONENT libraries
  )
else()
  add_library( ytlib STATIC
    ${YT_SOURCE_FILES}
  )
  add_library( ytlib-without-lfalloc STATIC
    ${YT_SOURCE_FILES}
  )
endif()

target_link_libraries ( ytlib
  ytext-lfalloc
  ytext-arcadia-util
  ytext-coro
  ytext-ev
  ytext-jerasure
  ytext-json
  protobuf
)

target_link_libraries ( ytlib-without-lfalloc
  ytext-lfalloc-fake
  ytext-arcadia-util
  ytext-coro
  ytext-ev
  ytext-jerasure
  ytext-json
  protobuf
)

if (HAVE_LIBUNWIND)
  target_link_libraries( ytlib unwind )
endif()

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_SOURCE_DIR}/contrib/tclap
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  .
)
