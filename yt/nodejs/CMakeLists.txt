set( SRCS
  common.cpp
  input_stream.cpp
  output_stream.cpp
  stream_base.cpp
)

add_library( yt_streams MODULE ${SRCS} yt_streams.cpp )
add_library( yt_driver MODULE yt_driver.cpp )
add_library( test_streams MODULE test_streams.cpp )

macro( NODE target )
  set_target_properties( ${target}
    PROPERTIES
    PREFIX ""
    SUFFIX ".node"
    COMPILE_DEFINITIONS "_GNU_SOURCE;_LARGEFILE_SOURCE;_FILE_OFFSET_BITS=64"
    LINK_FLAGS "-bundle -undefined dynamic_lookup"
  )
endmacro()

include_directories(
  "/usr/local/Cellar/node/0.6.18/include/node"
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/wscript.in
  ${CMAKE_CURRENT_BINARY_DIR}/wscript
)

NODE( yt_streams )
NODE( yt_driver )
NODE( test_streams )

