include_directories(
  ${CMAKE_BINARY_DIR}/include # for udfs
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/protocol_version.py.in
  ${PROJECT_SOURCE_DIR}/python/yt_proto/protocol_version.py
  @ONLY
)

add_subdirectory(build)
add_subdirectory(core)
add_subdirectory(core/test_framework)
add_subdirectory(ytlib)
add_subdirectory(client)
add_subdirectory(python)
add_subdirectory(tools)

if(YT_BUILD_ENABLE_SERVER_BINARIES)
  add_subdirectory(server)
endif()

if(YT_BUILD_ENABLE_TESTS)
  add_subdirectory(server/skynet_manager/unittests)
  add_subdirectory(core/unittests)
  add_subdirectory(ytlib/unittests)
  add_subdirectory(server/unittests)
else()
  message(STATUS "Omitting unittests from build (YT_BUILD_ENABLE_TESTS = OFF)")
endif()

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

if (YT_BUILD_ENABLE_CPP_TESTS)
  add_subdirectory(tests/cpp)
endif()
