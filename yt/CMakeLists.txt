include_directories(
  .
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/yt
  ${CMAKE_BINARY_DIR}/include
)

add_subdirectory(ytlib)
add_subdirectory(server)
add_subdirectory(driver)

if (YT_BUILD_ENABLE_EXPERIMENTS)
  add_subdirectory(experiments)
else()
  message(STATUS "Omitting experiments from build (YT_BUILD_ENABLE_EXPERIMENTS = OFF)")
endif()

if (YT_BUILD_ENABLE_TESTS)
  add_subdirectory(unittests)
else()
  message(STATUS "Omitting tests from build (YT_BUILD_ENABLE_TESTS = OFF)")
endif()

if (YT_BUILD_ENABLE_NODEJS)
  add_subdirectory(nodejs)
else()
  message(STATUS "Omitting NodeJS extensions from build (YT_BUILD_ENABLE_NODEJS = OFF)")
endif()

