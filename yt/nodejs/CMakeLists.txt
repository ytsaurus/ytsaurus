set( SRCS
  src/common.cpp
  src/stream_base.cpp
  src/input_stream.cpp
  src/output_stream.cpp
  src/driver.cpp
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/run_tests.sh.in
  ${CMAKE_CURRENT_BINARY_DIR}/run_tests.sh
)

function( NODE target )
  add_library( ${target} MODULE ${ARGN} )
  target_link_libraries( ${target} ytlib )

  set( _node_compile_definitions
    "_GNU_SOURCE;_LARGEFILE_SOURCE;_FILE_OFFSET_BITS=64")
  set( _node_link_flags
    "")

  if (APPLE)
    set( _node_link_flags "${_node_link_flags} -bundle -undefined dynamic_lookup" )
  endif()

  set_target_properties( ${target}
    PROPERTIES
    PREFIX ""
    SUFFIX ".node"
    COMPILE_DEFINITIONS "${_node_compile_definitions}"
    LINK_FLAGS "${_node_link_flags}"
  )
endfunction()

include_directories(
  "/usr/local/Cellar/node/0.6.18/include/node"
  "/usr/include/nodejs"
)

NODE( ytnode ${SRCS} )
NODE( ytnode_stubs src/ytnode_stubs.cpp )

