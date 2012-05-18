set( SRCS
  common.cpp
  input_stream.cpp
  output_stream.cpp
  stream_base.cpp
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/run_tests.sh.in
  ${CMAKE_CURRENT_BINARY_DIR}/run_tests.sh
)

add_library( ytnode_streams MODULE ${SRCS} yt_streams.cpp )
add_library( ytnode_driver MODULE yt_driver.cpp )
add_library( ytnode_test_streams MODULE test_streams.cpp )

macro( NODE target )
  set_target_properties( ${target}
    PROPERTIES
    PREFIX ""
    SUFFIX ".node"
    COMPILE_DEFINITIONS "_GNU_SOURCE;_LARGEFILE_SOURCE;_FILE_OFFSET_BITS=64"
    LINK_FLAGS "-bundle -undefined dynamic_lookup"
  )
  target_link_libraries( ${target} ytlib )
endmacro()

include_directories(
  "/usr/local/Cellar/node/0.6.18/include/node"
)

NODE( ytnode_streams )
NODE( ytnode_driver )
NODE( ytnode_test_streams )

