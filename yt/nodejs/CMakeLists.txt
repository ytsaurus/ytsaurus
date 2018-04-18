function(NODE target)
  add_library(${target} MODULE ${ARGN})
  target_link_libraries(${target}
    yt-library-client
    deps-arcadia-library-streams-lzop
    deps-arcadia-library-string_utils-base64
  )

  set(_node_compile_definitions "_GNU_SOURCE;_LARGEFILE_SOURCE;_FILE_OFFSET_BITS=64")
  set(_node_compile_flags "-fno-omit-frame-pointer")
  set(_node_link_flags "-static-libstdc++")

  if(APPLE)
    set(_node_link_flags "${_node_link_flags} -undefined dynamic_lookup")
  endif()

  if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    set(_node_link_flags "${_node_link_flags} -Wl,-exported_symbols_list,${CMAKE_CURRENT_SOURCE_DIR}/linker.map.darwin")
  else()
    set(_node_link_flags "${_node_link_flags} -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/linker.map")
  endif()

  set_target_properties(${target}
    PROPERTIES
    PREFIX ""
    SUFFIX ".node"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
    COMPILE_DEFINITIONS "${_node_compile_definitions}"
    COMPILE_FLAGS "${_node_compile_flags}"
    LINK_FLAGS "${_node_link_flags}"
  )

  install(
    TARGETS ${target}
    LIBRARY DESTINATION lib/node_modules/yt/lib
    PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
    COMPONENT nodejs
  )
endfunction()

include_directories(
  "/usr/local/include/node"
  "/usr/include/nodejs"
)

set( SRCS
  src/async_ref_counted.cpp
  src/common.cpp
  src/error.cpp
  src/future.cpp
  src/stream_base.cpp
  src/node.cpp
  src/input_stack.cpp
  src/input_stream.cpp
  src/input_stub.cpp
  src/output_stack.cpp
  src/output_stream.cpp
  src/output_stub.cpp
  src/driver.cpp
  src/stream_stack.cpp
  src/uv_invoker.cpp
)

NODE(ytnode ${SRCS} src/ytnode.cpp)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/run_tests.sh.in
  ${CMAKE_CURRENT_BINARY_DIR}/run_tests.sh
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/run_proxy.sh.in
  ${CMAKE_CURRENT_BINARY_DIR}/run_proxy.sh
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/package.json.in
  ${CMAKE_CURRENT_BINARY_DIR}/package.json
)

install(
  DIRECTORY bin/
  DESTINATION lib/node_modules/yt/bin
  FILE_PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
  COMPONENT nodejs
)

install(
  DIRECTORY lib/
  DESTINATION lib/node_modules/yt/lib
  COMPONENT nodejs
  FILES_MATCHING PATTERN "*.js"
)

install(
  DIRECTORY static/
  DESTINATION lib/node_modules/yt/static
  COMPONENT nodejs
  FILES_MATCHING
    PATTERN "*.css"
    PATTERN "*.mustache"
)

install(
  FILES index.js
  DESTINATION lib/node_modules/yt
  COMPONENT nodejs
)

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/package.json
  DESTINATION lib/node_modules/yt
  COMPONENT nodejs
)

install(
  SCRIPT "CMakeLists.Install.txt"
  COMPONENT nodejs
)
