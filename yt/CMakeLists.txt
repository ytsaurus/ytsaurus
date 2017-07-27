include_directories(
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include # for udfs
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/contrib/tclap # header-only library
  ${CMAKE_SOURCE_DIR}/contrib/libs/sparsehash/src # header-only library
  ${CMAKE_SOURCE_DIR}/contrib/libs/re2 # re2 contains relative includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf # protobuf contains relative includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/google/protobuf # protobuf contains relative includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/openssl/include # openssl contains relative includes
)

add_subdirectory(build)
add_subdirectory(core)
add_subdirectory(core/test_framework)
add_subdirectory(core/unittests)
add_subdirectory(ytlib)
add_subdirectory(ytlib/unittests)
add_subdirectory(server)
add_subdirectory(server/unittests)
add_subdirectory(run_codec)
add_subdirectory(python)

if(YT_BUILD_ENABLE_BENCHMARKS)
  add_subdirectory(benchmarks)
else()
  message(STATUS "Omitting benchmarks from build (YT_BUILD_ENABLE_BENCHMARKS = OFF)")
endif()

if(YT_BUILD_ENABLE_EXPERIMENTS)
  add_subdirectory(experiments)
else()
  message(STATUS "Omitting experiments from build (YT_BUILD_ENABLE_EXPERIMENTS = OFF)")
endif()

if(YT_BUILD_ENABLE_NODEJS)
  add_subdirectory(nodejs)
else()
  message(STATUS "Omitting NodeJS bindings from build (YT_BUILD_ENABLE_NODEJS = OFF)")
endif()
