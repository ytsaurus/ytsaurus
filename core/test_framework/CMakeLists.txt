include(Sources.txt)

add_library(yt-core-test-framework
  ${YT_SRCS}
)

target_include_directories(yt-core-test-framework
  PUBLIC
  ${CMAKE_SOURCE_DIR}/contrib/libs/gtest
  ${CMAKE_SOURCE_DIR}/contrib/libs/gmock
)

target_link_libraries(yt-core-test-framework
  yt-core
  deps-arcadia-contrib-libs-gtest
  deps-arcadia-contrib-libs-gmock
)
