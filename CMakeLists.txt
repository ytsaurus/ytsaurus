include_directories(
  ${CMAKE_BINARY_DIR}/include # for udfs
)

add_subdirectory(build)
add_subdirectory(core)
add_subdirectory(core/test_framework)
add_subdirectory(ytlib)
add_subdirectory(run_codec)
add_subdirectory(python)

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

if(CMAKE_VERSION VERSION_GREATER "3.0")
  file(LOCK ${CMAKE_CURRENT_LIST_DIR}/tests/integration/build_type.py GUARD FILE)
endif()
file(WRITE ${CMAKE_CURRENT_LIST_DIR}/tests/integration/build_type.py "BUILD_TYPE = \"${CMAKE_BUILD_TYPE}\"")
