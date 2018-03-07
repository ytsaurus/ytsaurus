add_executable(test_native_api test_native_api.cpp)

include_directories(
  ${CMAKE_SOURCE_DIR}
)

target_link_libraries(test_native_api
  deps-arcadia-library-lfalloc
  yt-library-client
  yt-core-test-framework
)
