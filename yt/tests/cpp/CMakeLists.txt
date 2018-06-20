add_executable(test_native_api test_native_api.cpp)

include_directories(
  ${CMAKE_SOURCE_DIR}
)

target_link_libraries(test_native_api
  yt-core-alloc
  yt-core-test-framework
  yt-library-client
)
